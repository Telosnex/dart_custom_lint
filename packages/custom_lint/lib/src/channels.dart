import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:analyzer_plugin/protocol/protocol.dart';
import 'package:analyzer_plugin/protocol/protocol_generated.dart';
// ignore: implementation_imports, not exported
import 'package:analyzer_plugin/src/protocol/protocol_internal.dart'
    show ResponseResult;

/// An interface for interacting with analyzer_plugin
abstract class AnalyzerPluginClientChannel {
  /// The list of messages sent by analyzer_plugin.
  Stream<Object?> get messages;

  /// Sends a JSON object to the analyzer_plugin server
  void sendJson(Map<String, Object?> json);

  /// Sends a [Response] to the analyzer_plugin server.
  void sendResponse({
    ResponseResult? data,
    RequestError? error,
    required String requestID,
    required int requestTime,
  }) {
    sendJson(
      Response(
        requestID,
        requestTime,
        result: data?.toJson(),
        error: error,
      ).toJson(),
    );
  }

  /// Releases the resources
  Future<void> close();
}

/// The number of bytes used to store the length of a message
const _lengthBytes = 4;

/// An interface for discussing with analyzer_plugin using a [SendPort]
class JsonSendPortChannel extends AnalyzerPluginClientChannel {
  /// An interface for discussing with analyzer_plugin using a [SendPortƒ]
  JsonSendPortChannel(this._sendPort) : _receivePort = ReceivePort() {
    _sendPort.send(_receivePort.sendPort);
  }

  final SendPort _sendPort;
  final ReceivePort _receivePort;

  @override
  late final Stream<Object?> messages = _receivePort.asBroadcastStream();

  @override
  void sendJson(Map<String, Object?> json) {
    _sendPort.send(json);
  }

  @override
  Future<void> close() async {
    _receivePort.close();
  }
}

/// An interface for discussing with analyzer_plugin using web sockets
class JsonSocketChannel extends AnalyzerPluginClientChannel {
  /// An interface for discussing with analyzer_plugin using web sockets
  JsonSocketChannel(this._socket) {
    // Pipe the socket messages in a broadcast stream
    _subscription = Stream.fromFuture(_socket).asyncExpand((e) => e).listen(
      (chunk) {
        try {
          for (final message in _receiveWithLength(chunk)) {
            _controller.add(jsonDecode(utf8.decode(message)));
          }
        } catch (error, stackTrace) {
          _controller.addError(error, stackTrace);
        }
      },
      onError: _controller.addError,
      onDone: _controller.close,
    );
  }

  final Future<Socket> _socket;

  final _controller = StreamController<Object?>.broadcast();
  late StreamSubscription<Uint8List> _subscription;

  Uint8List _pendingBytes = Uint8List(0);
  int? _pendingMessageLength;

  /// Send a message while having the first 4 bytes of the message be the length of the message.
  void _sendWithLength(Socket socket, List<int> data) {
    final length = data.length;
    final buffer = Uint8List(_lengthBytes + length);
    final byteData = ByteData.view(buffer.buffer);

    byteData.setUint32(0, length);
    buffer.setRange(_lengthBytes, _lengthBytes + length, data);
    socket.add(buffer);
  }

  /// The [sendJson] method have messages start with the message length,
  /// because a chunk of data can contain multiple separate messages.
  ///
  /// By sending the length with every message, the receiver can know
  /// where a message ends and another begins.
  Iterable<List<int>> _receiveWithLength(Uint8List input) sync* {
    final buffer = _pendingBytes.isEmpty
        ? input
        : (Uint8List(_pendingBytes.length + input.length)
          ..setAll(0, _pendingBytes)
          ..setAll(_pendingBytes.length, input));

    var startOffset = 0;

    while (true) {
      final messageLength = _pendingMessageLength;
      if (messageLength == null) {
        if (buffer.length - startOffset < _lengthBytes) break;

        // Reading the length of the next message.
        _pendingMessageLength = ByteData.sublistView(
          buffer,
          startOffset,
          startOffset + _lengthBytes,
        ).getUint32(0);

        // We have the message length, now reading the message.
        startOffset += _lengthBytes;
        continue;
      }

      if (buffer.length - startOffset < messageLength) break;

      // We have the full message.
      yield buffer.sublist(startOffset, startOffset + messageLength);

      // Reset to reading the length of the next message.
      startOffset += messageLength;
      _pendingMessageLength = null;
    }

    _pendingBytes = Uint8List.sublistView(buffer, startOffset);
  }

  @override
  late final Stream<Object?> messages = _controller.stream;

  @override
  Future<void> sendJson(Map<String, Object?> json) async {
    final socket = await _socket;

    _sendWithLength(
      socket,
      utf8.encode(jsonEncode(json)),
    );
  }

  @override
  Future<void> close() async {
    await Future.wait([
      _subscription
          .cancel()
          .timeout(const Duration(seconds: 1), onTimeout: () {}),
      _controller.close(),
    ]);
  }
}
