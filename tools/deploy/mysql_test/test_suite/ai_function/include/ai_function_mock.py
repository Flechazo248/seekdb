#!/usr/bin/env python3
"""Deterministic mock AI service for the ai_function mysqltests.

It implements the provider protocols used by
tools/deploy/mysql_test/test_suite/ai_function/t/ai_function_enhancement.test :

  POST /compatible-mode/v1/chat/completions  -> OpenAI 兼容的 completion
  POST /compatible-mode/v1/embeddings          -> OpenAI 兼容的 embedding
  POST /v1/rerank                              -> siliconflow 兼容的 rerank
  POST /api/generate                           -> ollama 原生 completion
  POST /api/embed                              -> ollama 原生 embedding

用法:
    python3 ai_function_mock.py start 8000    # 后台启动
    python3 ai_function_mock.py stop  8000    # 停止
    python3 ai_function_mock.py serve 8000    # 前台运行
    python3 ai_function_mock.py events 8000   # 打印已记录的请求
"""

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

EMBED_DIM = 768


def state_dir(port):
    return os.path.join(tempfile.gettempdir(), "seekdb_ai_function_mock_%d" % port)


def state_path(port, name):
    return os.path.join(state_dir(port), name)


def read_events(port):
    events = []
    path = state_path(port, "events.jsonl")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as stream:
            for line in stream:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
    return events


def stop_server(port):
    pid_path = state_path(port, "server.pid")
    if not os.path.exists(pid_path):
        return
    try:
        with open(pid_path, "r", encoding="ascii") as stream:
            pid = int(stream.read().strip())
        os.kill(pid, signal.SIGTERM)
        for _ in range(50):
            try:
                os.kill(pid, 0)
            except ProcessError:
                break
            time.sleep(0.02)
    except (ValueError, ProcessError):
        pass
    try:
        os.unlink(pid_path)
    except FileNotFoundError:
        pass


def start_server(port):
    os.makedirs(state_dir(port), exist_ok=True)
    stop_server(port)
    for name in ("events.jsonl", "server.out"):
        with open(state_path(port, name), "w", encoding="utf-8"):
            pass
    output = open(state_path(port, "server.out"), "a", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, os.path.abspath(__file__), "serve", str(port)],
        stdin=subprocess.DEVNULL,
        stdout=output,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    with open(state_path(port, "server.pid"), "w", encoding="ascii") as stream:
        stream.write(str(process.pid))
    for _ in range(100):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                return
        except OSError:
            if process.poll() is not None:
                break
            time.sleep(0.02)
    raise RuntimeError("AI function mock did not start on port %d" % port)


def make_vector(text, index):
    """确定性的 %d 维向量, 便于断言维度与顺序""" % EMBED_DIM
    base = float(index + 1)
    try:
        base = float(str(text))
    except (TypeError, ValueError):
        pass
    vector = [base]
    while len(vector) < EMBED_DIM:
        vector.append(0.25)
    return vector


def serve(port):
    os.makedirs(state_dir(port), exist_ok=True)
    lock = threading.Lock()
    log_path = state_path(port, "events.jsonl")

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt, *args):
            return

        def do_POST(self):
            size = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(size)
            try:
                body = json.loads(raw.decode("utf-8"))
            except Exception:
                body = {}
            parsed = urlparse(self.path)
            path = parsed.path
            model = str(body.get("model", ""))

            status = 200
            response = {}

            if path.endswith("/chat/completions"):
                prompt = ""
                for message in body.get("messages", []):
                    if message.get("role") == "user":
                        prompt = str(message.get("content", ""))
                response = {
                    "id": "chatcmpl-mock",
                    "object": "chat.completion",
                    "created": 1735120033,
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "stop",
                            "message": {"role": "assistant", "content": "mock:" + prompt},
                        }
                    ],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
                }
            elif path.endswith("/embeddings"):
                values = body.get("input", [])
                if not isinstance(values, list):
                    values = [values]
                response = {
                    "id": "mock-embed",
                    "object": "list",
                    "model": model,
                    "data": [
                        {"index": index, "object": "embedding", "embedding": make_vector(value, index)}
                        for index, value in enumerate(values)
                    ],
                    "usage": {"prompt_tokens": len(values), "total_tokens": len(values)},
                }
            elif path.endswith("/rerank"):
                documents = body.get("documents", [])
                results = []
                for index, document in enumerate(documents):
                    if isinstance(document, dict):
                        text = ""
                        for value in document.values():
                            text = str(value)
                            break
                    else:
                        text = str(document)
                    # 分数随下标递减, 保证排序后与原始顺序一致
                    results.append(
                        {"index": index, "relevance_score": round(1.0 - index * 0.1, 6), "document": {"text": text}}
                    )
                response = {"id": "mock-rerank", "model": model, "results": results}
            elif path.endswith("/api/generate"):
                # ollama 原生 completion
                response = {
                    "model": model,
                    "response": "mock:" + str(body.get("prompt", "")),
                    "done": True,
                }
            elif path.endswith("/api/embed"):
                # ollama 原生 embedding
                values = body.get("input", "")
                if not isinstance(values, list):
                    values = [values]
                response = {
                    "model": model,
                    "embeddings": [make_vector(value, index) for index, value in enumerate(values)],
                }
            else:
                status = 404
                response = {"error": {"message": "unsupported path: %s" % path}}

            event = {
                "path": path,
                "status": status,
                "model": model,
                "authorization": self.headers.get("Authorization", ""),
                "content_type": self.headers.get("Content-Type", ""),
                "body": body,
            }
            with lock:
                with open(log_path, "a", encoding="utf-8") as stream:
                    stream.write(json.dumps(event, ensure_ascii=False) + "\n")

            payload = json.dumps(response, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        return 1
    action = sys.argv[1]
    port = int(sys.argv[2])
    if action == "start":
        start_server(port)
        print("mock ai service started on port %d" % port)
    elif action == "stop":
        stop_server(port)
        print("mock ai service stopped")
    elif action == "serve":
        serve(port)
    elif action == "events":
        for event in read_events(port):
            print(json.dumps(event, ensure_ascii=False))
    else:
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
