import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:analyzer_plugin/protocol/protocol.dart';
import 'package:analyzer_plugin/protocol/protocol_generated.dart';
import 'package:meta/meta.dart';
import 'package:path/path.dart';
import 'package:uuid/uuid.dart';

import '../async_operation.dart';
import '../channels.dart';
import '../workspace.dart';
import 'custom_lint_analyzer_plugin.dart';
import 'protocol.dart';

const _clientShutdownTimeout = Duration(seconds: 2);
const _clientExitTimeout = Duration(seconds: 2);
const _socketCloseTimeout = Duration(seconds: 1);

Future<T> _asyncRetry<T>(
  Future<T> Function() cb, {
  required int retryCount,
}) async {
  var i = 0;
  while (true) {
    i++;
    try {
      return await cb();
    } catch (error, stackTrace) {
      // Logging the error
      Zone.current.handleUncaughtError(error, stackTrace);
      // If out of retry, stop
      if (i >= retryCount) rethrow;
    }
  }
}

/// Best-effort sends a `plugin.shutdown` request to the client process.
@visibleForTesting
Future<void> sendBestEffortPluginShutdown(
  Future<Response> Function(Request request) sendRequest, {
  Duration timeout = _clientShutdownTimeout,
}) async {
  try {
    await sendRequest(PluginShutdownParams().toRequest(const Uuid().v4()))
        .timeout(timeout);
  } catch (_) {
    // Shutdown is best-effort. The caller will still close sockets/kill the
    // process if the client does not respond.
  }
}

/// Terminates a custom_lint client process and waits for it to exit.
@visibleForTesting
Future<void> terminateCustomLintClientProcess(
  Process? process, {
  Duration timeout = _clientExitTimeout,
}) async {
  if (process == null) return;

  process.kill();

  try {
    await process.exitCode.timeout(timeout);
    return;
  } on TimeoutException {
    if (!Platform.isWindows) {
      process.kill(ProcessSignal.sigkill);
    }
  }

  try {
    await process.exitCode.timeout(timeout);
  } on TimeoutException {
    // Nothing more to do. We attempted SIGTERM, and SIGKILL where available.
  }
}

/// An interface for communicating with the plugins over a [Socket].
class SocketCustomLintServerToClientChannel {
  SocketCustomLintServerToClientChannel._(
    this._server,
    this._serverSocket,
    this._socket,
    this._version,
    this._contextRoots,
    this._workspace,
  ) : _channel = JsonSocketChannel(_socket);

  /// Starts a server socket and exposes a way to communicate with potential clients.
  ///
  /// Returns `null` if the workspace has no plugins enabled.
  static Future<SocketCustomLintServerToClientChannel?> create(
    CustomLintServer server,
    PluginVersionCheckParams version,
    AnalysisSetContextRootsParams contextRoots, {
    required Directory workingDirectory,
  }) async {
    final workspace = await CustomLintWorkspace.fromContextRoots(
      contextRoots.roots,
      workingDirectory: workingDirectory,
    );
    if (workspace.uniquePluginNames.isEmpty) return null;

    final serverSocket = await _createServerSocket();

    return SocketCustomLintServerToClientChannel._(
      server,
      serverSocket,
      // Voluntarily thow if no client connected
      serverSocket.safeFirst,
      version,
      contextRoots,
      workspace,
    );
  }

  Directory? _tempDirectory;

  final Future<Socket> _socket;
  final JsonSocketChannel _channel;
  final CustomLintServer _server;
  final PluginVersionCheckParams _version;
  final ServerSocket _serverSocket;
  late final Future<Process?> _processFuture;
  final CustomLintWorkspace _workspace;

  AnalysisSetContextRootsParams _contextRoots;

  late final Stream<CustomLintMessage> _messages = _channel.messages
      .map((e) => e! as Map<String, Object?>)
      .map(CustomLintMessage.fromJson);

  late final Stream<CustomLintResponse> _responses = _messages
      .where((msg) => msg is CustomLintMessageResponse)
      .cast<CustomLintMessageResponse>()
      .map((e) => e.response);

  /// The events sent by the client.
  late final Stream<CustomLintEvent> events = _messages
      .where((msg) => msg is CustomLintMessageEvent)
      .cast<CustomLintMessageEvent>()
      .map((eventMsg) => eventMsg.event);

  static Future<ServerSocket> _createServerSocket() async {
    try {
      return await ServerSocket.bind(InternetAddress.loopbackIPv6, 0);
    } on SocketException {
      return ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    }
  }

  /// Initializes and waits for the client to start
  Future<void> init({required bool debug}) async {
    _processFuture = _startProcess(debug: debug);
    final process = await _processFuture;
    // No process started, likely because no plugin were detected. We can stop here.
    if (process == null) return;

    final out = process.stdout.map(utf8.decode);

    out.listen((event) => _server.handlePrint(event, isClientMessage: true));
    process.stderr
        .map(utf8.decode)
        .listen((e) => _server.handleUncaughtError(e, StackTrace.empty));

    // Checking process failure _after_ piping stdout/stderr to the log files.
    // This is so that if client failed to boot, logs in it should still be available
    await _checkInitializationFail(process);

    await Future.wait([
      sendAnalyzerPluginRequest(_version.toRequest(const Uuid().v4())),
      sendAnalyzerPluginRequest(_contextRoots.toRequest(const Uuid().v4())),
    ]);
  }

  /// Updates the context roots on the client
  Future<AnalysisSetContextRootsResult> setContextRoots(
    AnalysisSetContextRootsParams contextRoots,
  ) async {
    _contextRoots = contextRoots;
    return AnalysisSetContextRootsResult();
  }

  /// Encapsulates all the logic for initializing the process,
  /// without setting up the connection.
  ///
  /// Will throw if the process fails to start.
  Future<Process?> _startProcess({
    required bool debug,
  }) async {
    final tempDir = _tempDirectory =
        Directory.systemTemp.createTempSync('custom_lint_client');

    try {
      await _workspace.resolvePluginHost(tempDir);
      _writeEntrypoint(_workspace.uniquePluginNames, tempDir);

      return _asyncRetry(retryCount: 5, () async {
        final process = await Process.start(
          'dart',
          [
            if (_server.watchMode ?? debug) '--enable-vm-service=0',
            join('lib', 'custom_lint_client.dart'),
            _serverSocket.address.host,
            _serverSocket.port.toString(),
          ],
          workingDirectory: tempDir.path,
        );
        return process;
      });
    } catch (_) {
      // If the process failed to start, we can delete the temp directory
      await _tempDirectory?.delete(recursive: true);
      rethrow;
    }
  }

  void _writeEntrypoint(
    Iterable<String> pluginNames,
    Directory tempDirectory,
  ) {
    final imports = pluginNames
        .map((name) => "import 'package:$name/$name.dart' as $name;\n")
        .join();

    final plugins = pluginNames
        .map((pluginName) => "'$pluginName': $pluginName.createPlugin,\n")
        .join();

    final mainFile = File(
      join(tempDirectory.path, 'lib', 'custom_lint_client.dart'),
    );
    mainFile.createSync(recursive: true);
    mainFile.writeAsStringSync('''
import 'dart:convert';
import 'dart:io';
import 'package:custom_lint_builder/src/channel.dart';
$imports

void main(List<String> args) async {
  final host = args[0];
  final port = int.parse(args[1]);

  runSocket(
    port: port,
    host: host,
    fix: ${_server.fix},
    includeBuiltInLints: ${_server.includeBuiltInLints},
    {$plugins},
  );
}
''');
  }

  Future<void> _checkInitializationFail(Process process) async {
    var running = true;
    try {
      return await Future.any<void>([
        _socket,
        process.exitCode.then((exitCode) async {
          // A socket was returned before the exitCode was obtained.
          // As such, the process correctly started
          if (!running) return;

          await _server.handlePluginInitializationFail();

          throw StateError('Failed to start the plugins.');
        }),
      ]);
    } finally {
      running = false;
    }
  }

  /// Sends a request based on the analyzer_plugin protocol, expecting
  /// an analyzer_plugin response.
  Future<Response> sendAnalyzerPluginRequest(Request request) async {
    final response = await sendCustomLintRequest(
      CustomLintRequest.analyzerPluginRequest(request, id: request.id),
    );

    return switch (response) {
      CustomLintResponseAnalyzerPluginResponse() => response.response,
      CustomLintResponseAwaitAnalysisDone() ||
      CustomLintResponsePong() ||
      CustomLintResponseError() =>
        throw UnsupportedError(
          'Expected a CustomLintResponse.analyzerPluginResponse '
          'but received ${response.runtimeType}.',
        )
    };
  }

  /// Sends a custom_lint request to the client, expecting a custom_lint response
  Future<CustomLintResponse> sendCustomLintRequest(
    CustomLintRequest request,
  ) async {
    final matchingResponse = _responses.firstWhere(
      (e) => e.id == request.id,
      orElse: () => throw StateError(
        'No response for request $request',
      ),
    );

    await _channel.sendJson(request.toJson());

    final response = await matchingResponse;

    switch (response) {
      case CustomLintResponseAwaitAnalysisDone():
      case CustomLintResponsePong():
        break;

      case CustomLintResponseAnalyzerPluginResponse():
        final error = response.response.error;
        if (error != null) {
          throw CustomLintRequestFailure(
            message: error.message,
            stackTrace: error.stackTrace,
            request: request,
          );
        }
      case CustomLintResponseError():
        throw CustomLintRequestFailure(
          message: response.message,
          stackTrace: response.stackTrace,
          request: request,
        );
    }

    return response;
  }

  /// Stops the client, liberating the resources.
  Future<void> close() async {
    await sendBestEffortPluginShutdown(sendAnalyzerPluginRequest);

    await Future.wait([
      if (_tempDirectory != null) _tempDirectory!.delete(recursive: true),
      _serverSocket.close(),
      _closeSocket(),
      _channel.close(),
      _processFuture.then<void>(
        terminateCustomLintClientProcess,
        // The process wasn't started. No need to do anything.
        onError: (_) {},
      ),
    ]);
  }

  Future<void> _closeSocket() async {
    Socket? socket;
    try {
      socket = await _socket.timeout(_socketCloseTimeout);
      await socket.close().timeout(_socketCloseTimeout);
    } catch (_) {
      socket?.destroy();
    } finally {
      socket?.destroy();
    }
  }
}

/// A custom_lint request failed
class CustomLintRequestFailure implements Exception {
  /// A custom_lint request failed
  CustomLintRequestFailure({
    required this.message,
    required this.stackTrace,
    required this.request,
  });

  /// The error message
  final String message;

  /// The stacktrace of the error
  final String? stackTrace;

  /// The request that failed.
  final CustomLintRequest request;

  @override
  String toString() {
    return 'A request threw the exception:$message\n$stackTrace';
  }
}
