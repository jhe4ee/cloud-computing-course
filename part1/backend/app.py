from flask import Flask, jsonify
import redis
import os

app = Flask(__name__)

redis_host = os.environ.get("REDIS_HOST", "localhost")
redis_port = int(os.environ.get("REDIS_PORT", 6379))
redis_password = os.environ.get("REDIS_PASSWORD", "")

def get_redis():
    kwargs = {"host": redis_host, "port": redis_port, "decode_responses": True}
    if redis_password:
        kwargs["password"] = redis_password
    return redis.Redis(**kwargs)


@app.route("/api/ping")
def ping():
    return jsonify({"status": "ok"})


@app.route("/api/health")
def health():
    try:
        r = get_redis()
        r.ping()
        return jsonify({"redis": "connected"})
    except Exception as e:
        return jsonify({"redis": "error", "detail": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
