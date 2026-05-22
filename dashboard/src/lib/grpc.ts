import { createClient } from "@connectrpc/connect";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { KvService } from "@/proto/kv_pb";

const GRPC_WEB_URL =
  process.env.NEXT_PUBLIC_GRPC_WEB_URL || "http://localhost:8080";

const transport = createGrpcWebTransport({
  baseUrl: GRPC_WEB_URL,
});

export const kvClient = createClient(KvService, transport);
