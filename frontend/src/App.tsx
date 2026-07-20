import { useEffect, useState } from "react";
import "./App.css";
import { ApiError, checkHealth, classifyText, type ClassifyResponse } from "./api";

// Stable per-tab session id so a user's turns land on the same Kafka
// partition/session for the lifetime of this browser tab (see
// pipeline/kafka_producer.py, which keys by session_id).
const SESSION_ID = crypto.randomUUID();

type HealthState =
  | { state: "loading" }
  | { state: "ok"; kafkaEnabled: boolean; kafkaConnected: boolean; model: string }
  | { state: "down" };

export default function App() {
  const [text, setText] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [results, setResults] = useState<ClassifyResponse[]>([]);
  const [health, setHealth] = useState<HealthState>({ state: "loading" });

  useEffect(() => {
    checkHealth()
      .then((h) =>
        setHealth({
          state: "ok",
          kafkaEnabled: h.kafka_enabled,
          kafkaConnected: h.kafka_connected,
          model: h.model,
        }),
      )
      .catch(() => setHealth({ state: "down" }));
  }, []);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = text.trim();
    if (!trimmed || submitting) return;

    setSubmitting(true);
    setError(null);
    try {
      const result = await classifyText(SESSION_ID, trimmed);
      setResults((prev) => [result, ...prev]);
      setText("");
    } catch (err) {
      if (err instanceof ApiError) {
        setError(err.message);
      } else {
        setError("Couldn't reach the classification API. Is the backend running?");
      }
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="app">
      <div className="header">
        <h1>EmbraceAI — classification demo</h1>
        <p>
          Sends text to a DistilBERT model through the Hugging Face Inference API and shows the
          predicted category. This is a demo for the model + pipeline, not the counseling product itself.
        </p>
      </div>

      <div className="statusRow">
        <span className={`dot ${health.state === "ok" ? "ok" : health.state === "down" ? "down" : ""}`} />
        {health.state === "loading" && "checking backend..."}
        {health.state === "down" && "backend unreachable"}
        {health.state === "ok" &&
          `backend up · model: ${health.model} · kafka: ${
            health.kafkaEnabled ? (health.kafkaConnected ? "connected" : "enabled, not connected") : "disabled"
          }`}
      </div>

      <form onSubmit={handleSubmit}>
        <textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="Type a message to classify..."
          maxLength={2000}
        />
        <button type="submit" disabled={submitting || !text.trim()}>
          {submitting ? "Classifying..." : "Classify"}
        </button>
        {error && <p className="error">{error}</p>}
      </form>

      <div className="results">
        {results.map((r, i) => (
          <div className="resultCard" key={i}>
            <div className="meta">
              <span>session {r.session_id.slice(0, 8)}</span>
              <span>{r.model}</span>
            </div>
            <div className="text">&ldquo;{r.user_text}&rdquo;</div>
            {r.predictions.slice(0, 5).map((p) => (
              <div className="predictionBar" key={p.label}>
                <span className="label">{p.label}</span>
                <span className="track">
                  <span className="fill" style={{ width: `${Math.round(p.score * 100)}%` }} />
                </span>
                <span className="score">{Math.round(p.score * 100)}%</span>
              </div>
            ))}
            <span className={`kafkaTag ${r.published_to_kafka ? "" : "skipped"}`}>
              {r.published_to_kafka ? "published to Kafka" : "Kafka publish skipped"}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
