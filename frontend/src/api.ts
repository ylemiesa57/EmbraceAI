export interface ClassificationResult {
  label: string;
  score: number;
}

export interface ClassifyResponse {
  session_id: string;
  user_text: string;
  predictions: ClassificationResult[];
  top_label: string;
  model: string;
  published_to_kafka: boolean;
}

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    this.name = "ApiError";
  }
}

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

export async function classifyText(
  sessionId: string,
  userText: string,
): Promise<ClassifyResponse> {
  const res = await fetch(`${API_BASE_URL}/api/classify`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId, user_text: userText }),
  });

  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = body.detail ?? detail;
    } catch {
      // response wasn't JSON; fall back to statusText
    }
    throw new ApiError(res.status, detail);
  }

  return res.json();
}

export async function checkHealth(): Promise<{
  status: string;
  model: string;
  kafka_enabled: boolean;
  kafka_connected: boolean;
}> {
  const res = await fetch(`${API_BASE_URL}/api/health`);
  if (!res.ok) {
    throw new ApiError(res.status, res.statusText);
  }
  return res.json();
}
