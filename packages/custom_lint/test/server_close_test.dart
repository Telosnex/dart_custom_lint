import 'dart:async';
import 'dart:io';
import 'dart:isolate';

import 'package:analyzer_plugin/protocol/protocol.dart';
import 'package:analyzer_plugin/protocol/protocol_generated.dart';
import 'package:custom_lint/src/plugin_delegate.dart';
import 'package:custom_lint/src/server_isolate_channel.dart';
import 'package:custom_lint/src/v2/custom_lint_analyzer_plugin.dart';
import 'package:custom_lint/src/v2/server_to_client_channel.dart';
import 'package:custom_lint/src/workspace.dart' as workspace;
import 'package:path/path.dart' as p;
import 'package:test/test.dart';

void main() {
  group('CustomLintServer.close', () {
    test('does not wait forever for in-flight requests before closing clients',
        () async {
      final channel = ServerIsolateChannel();
      addTearDown(channel.close);

      final server = await _startServer(channel.receivePort.sendPort);

      // This request is forwarded to the custom_lint client. Since this test
      // never sends analysis.setContextRoots, no client channel exists yet and
      // the request would wait on _clientChannel.safeFirst forever.
      final pendingRequest = channel
          .sendRequest(EditGetFixesParams('/tmp/file.dart', 0))
          .then<void>((_) {}, onError: (_) {});

      await Future<void>.delayed(const Duration(milliseconds: 10));

      await expectLater(
        server.close().timeout(const Duration(milliseconds: 300)),
        completes,
      );
      await pendingRequest.timeout(const Duration(milliseconds: 300));
    });

    test('does not add client channel errors after shutdown starts', () async {
      final previousRunProcess = workspace.runProcess;
      addTearDown(() => workspace.runProcess = previousRunProcess);

      final pubGetStarted = Completer<void>();
      final failPubGet = Completer<void>();
      workspace.runProcess = (
        executable,
        arguments, {
        workingDirectory,
        environment,
        includeParentEnvironment = true,
        runInShell = false,
        stdoutEncoding,
        stderrEncoding,
      }) async {
        if (!pubGetStarted.isCompleted) pubGetStarted.complete();
        await failPubGet.future;
        return ProcessResult(0, 1, '', 'pub get failed');
      };

      final channel = ServerIsolateChannel();
      addTearDown(channel.close);

      final server = await _startServer(channel.receivePort.sendPort);
      final app = _createProjectWithPlugin();

      await channel.sendRequest(
        PluginVersionCheckParams('', '', '1.0.0-alpha.0'),
      );

      final setContextRoots = channel
          .sendRequest(
            AnalysisSetContextRootsParams([ContextRoot(app.path, [])]),
          )
          .then<void>((_) {}, onError: (_) {});

      await pubGetStarted.future.timeout(const Duration(milliseconds: 300));

      final close = server.close();
      await Future<void>.delayed(const Duration(milliseconds: 10));
      failPubGet.complete();

      await expectLater(
        close.timeout(const Duration(seconds: 5)),
        completes,
      );
      await setContextRoots.timeout(const Duration(milliseconds: 300));
    });

    test('does not wait for a first client channel during shutdown', () async {
      final receivePort = ReceivePort();
      addTearDown(receivePort.close);

      final server = await _startServer(receivePort.sendPort);

      await expectLater(
        server.close().timeout(const Duration(milliseconds: 300)),
        completes,
      );
    });
  });

  group('custom_lint client process shutdown', () {
    test('sends plugin.shutdown as a best-effort request', () async {
      final requests = <Request>[];

      await sendBestEffortPluginShutdown(
        (request) {
          requests.add(request);
          return Completer<Response>().future;
        },
        timeout: const Duration(milliseconds: 10),
      );

      expect(requests, hasLength(1));
      expect(
        requests.single.method,
        PluginShutdownParams().toRequest('').method,
      );
    });

    test('kills the client and waits for exit, escalating if needed', () async {
      final process = _FakeProcess();

      await terminateCustomLintClientProcess(
        process,
        timeout: const Duration(milliseconds: 10),
      );

      expect(
        process.killSignals,
        [ProcessSignal.sigterm, ProcessSignal.sigkill],
      );
      expect(process.exitCodeCompleted, true);
    });
  });
}

Directory _createProjectWithPlugin() {
  final parent =
      Directory.systemTemp.createTempSync('custom_lint_close_project');
  addTearDown(() async {
    if (parent.existsSync()) {
      await parent.delete(recursive: true);
    }
  });

  final plugin = Directory(p.join(parent.path, 'test_lint'))..createSync();
  File(p.join(plugin.path, 'pubspec.yaml')).writeAsStringSync('''
name: test_lint
version: 0.0.1
publish_to: none

environment:
  sdk: ">=3.0.0 <4.0.0"

dependencies:
  custom_lint_builder: any
''');

  final app = Directory(p.join(parent.path, 'app'))..createSync();
  File(p.join(app.path, 'pubspec.yaml')).writeAsStringSync('''
name: app
version: 0.0.1
publish_to: none

environment:
  sdk: ">=3.0.0 <4.0.0"

dev_dependencies:
  test_lint:
    path: ${plugin.path}
''');
  File(p.join(app.path, '.dart_tool', 'package_config.json'))
    ..createSync(recursive: true)
    ..writeAsStringSync('''
{
  "configVersion": 2,
  "packages": [
    {
      "name": "app",
      "rootUri": "${app.uri}",
      "packageUri": "lib/",
      "languageVersion": "3.0"
    },
    {
      "name": "test_lint",
      "rootUri": "${plugin.uri}",
      "packageUri": "lib/",
      "languageVersion": "3.0"
    }
  ]
}
''');

  return app;
}

Future<CustomLintServer> _startServer(SendPort sendPort) async {
  final workingDirectory =
      Directory.systemTemp.createTempSync('custom_lint_server_close_test');
  addTearDown(() async {
    if (workingDirectory.existsSync()) {
      await workingDirectory.delete(recursive: true);
    }
  });

  final server = await CustomLintServer.start(
    sendPort: sendPort,
    watchMode: false,
    includeBuiltInLints: false,
    fix: false,
    delegate: const _NoopDelegate(),
    workingDirectory: workingDirectory,
  );
  addTearDown(server.close);

  return server;
}

class _NoopDelegate implements CustomLintDelegate {
  const _NoopDelegate();

  @override
  void pluginError(
    CustomLintServer serverPlugin,
    String err, {
    required String? stackTrace,
    required String pluginName,
    required List<ContextRoot>? pluginContextRoots,
  }) {}

  @override
  void pluginInitializationFail(
    CustomLintServer serverPlugin,
    String message, {
    required List<ContextRoot>? allContextRoots,
  }) {}

  @override
  void pluginMessage(
    CustomLintServer serverPlugin,
    String message, {
    required String? pluginName,
    required List<ContextRoot>? pluginContextRoots,
  }) {}

  @override
  void requestError(
    CustomLintServer serverPlugin,
    Request request,
    RequestError requestError, {
    required List<ContextRoot>? allContextRoots,
  }) {}

  @override
  void serverError(
    CustomLintServer serverPlugin,
    Object error,
    StackTrace stackTrace, {
    required List<ContextRoot>? allContextRoots,
  }) {}

  @override
  void serverMessage(
    CustomLintServer serverPlugin,
    String message, {
    required List<ContextRoot>? allContextRoots,
  }) {}
}

class _FakeProcess implements Process {
  final _exitCode = Completer<int>();

  final killSignals = <ProcessSignal>[];

  bool get exitCodeCompleted => _exitCode.isCompleted;

  @override
  bool kill([ProcessSignal signal = ProcessSignal.sigterm]) {
    killSignals.add(signal);
    if (signal == ProcessSignal.sigkill && !_exitCode.isCompleted) {
      _exitCode.complete(137);
    }
    return true;
  }

  @override
  Future<int> get exitCode => _exitCode.future;

  @override
  int get pid => 42;

  @override
  IOSink get stdin => IOSink(StreamController<List<int>>().sink);

  @override
  Stream<List<int>> get stdout => const Stream.empty();

  @override
  Stream<List<int>> get stderr => const Stream.empty();
}
