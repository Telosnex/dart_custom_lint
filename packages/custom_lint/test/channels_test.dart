import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:custom_lint/src/channels.dart';
import 'package:test/test.dart';

void main() {
  group('JsonSocketChannel', () {
    test('decodes a message when the length prefix is split across chunks',
        () async {
      final message = {'id': 'split-length', 'method': 'ping'};
      final frame = _encodeFrame(message);

      await expectLater(
        _readMessageFromChunks([
          Uint8List.sublistView(frame, 0, 2),
          Uint8List.sublistView(frame, 2),
        ]),
        completion(equals(message)),
      );
    });

    test('decodes a message when the JSON body is split across chunks',
        () async {
      final message = {
        'id': 'split-body',
        'method': 'ping',
        'params': {'value': 'large enough to split'},
      };
      final frame = _encodeFrame(message);

      await expectLater(
        _readMessageFromChunks([
          Uint8List.sublistView(frame, 0, 8),
          Uint8List.sublistView(frame, 8),
        ]),
        completion(equals(message)),
      );
    });
  });
}

Uint8List _encodeFrame(Map<String, Object?> message) {
  final body = utf8.encode(jsonEncode(message));
  final frame = Uint8List(4 + body.length);
  final byteData = ByteData.view(frame.buffer);

  byteData.setUint32(0, body.length);
  frame.setRange(4, frame.length, body);

  return frame;
}

Future<Object?> _readMessageFromChunks(List<Uint8List> chunks) async {
  final serverSocket = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
  final clientSocket = await Socket.connect(
    serverSocket.address,
    serverSocket.port,
  );
  clientSocket.setOption(SocketOption.tcpNoDelay, true);

  final channel = JsonSocketChannel(serverSocket.first);

  try {
    final message = channel.messages.first.timeout(
      const Duration(milliseconds: 300),
      onTimeout: () => throw TimeoutException(
        'JsonSocketChannel did not emit a decoded message.',
      ),
    );

    for (final chunk in chunks) {
      clientSocket.add(chunk);
      await clientSocket.flush();
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }

    return await message;
  } finally {
    clientSocket.destroy();
    await channel.close();
    await serverSocket.close();
  }
}
