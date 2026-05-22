// Authentication utilities
import bcrypt from "bcryptjs";

export interface Credentials {
  username: string;
  password: string;
}

// Get credentials from environment variables
export function getCredentials(): Credentials {
  return {
    username: process.env.DASHBOARD_USERNAME || "admin",
    password: process.env.DASHBOARD_PASSWORD || "admin",
  };
}

// Verify HTTP Basic Auth credentials
// For development: simple string comparison
// For production: use the bcrypt hash stored in env
export async function verifyCredentials(
  username: string,
  password: string
): Promise<boolean> {
  const { username: envUser, password: envPass } = getCredentials();

  // Check username first
  if (username !== envUser) {
    return false;
  }

  // Check if password is a hash (bcrypt hashes start with $2)
  if (envPass.startsWith("$2")) {
    // It's a bcrypt hash
    return bcrypt.compareSync(password, envPass);
  }

  // Plain text password (development only)
  return password === envPass;
}

// Parse HTTP Basic Auth header
export function parseBasicAuth(header: string | null): { username: string; password: string } | null {
  if (!header || !header.startsWith("Basic ")) {
    return null;
  }

  try {
    const base64Credentials = header.substring(6);
    const credentials = atob(base64Credentials);
    const [username, password] = credentials.split(":");

    return { username, password };
  } catch {
    return null;
  }
}
