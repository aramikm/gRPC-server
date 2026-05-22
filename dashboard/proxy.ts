import { NextResponse, type NextRequest } from "next/server";
import { parseBasicAuth, verifyCredentials } from "@/lib/auth";

export async function proxy(request: NextRequest) {
  const authHeader = request.headers.get("authorization");
  const parsed = parseBasicAuth(authHeader);

  if (!parsed) {
    return challenge();
  }

  const isValid = await verifyCredentials(parsed.username, parsed.password);
  if (!isValid) {
    return challenge();
  }

  return NextResponse.next();
}

function challenge() {
  return new NextResponse("Unauthorized", {
    status: 401,
    headers: {
      "WWW-Authenticate": 'Basic realm="Dashboard"',
    },
  });
}

export const config = {
  matcher: ["/((?!_next|favicon.ico|static).*)"],
};
