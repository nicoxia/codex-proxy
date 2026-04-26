import { isIP } from "node:net";
import { isLocalhostRequest } from "./is-localhost.js";

function normalize(addr: string): string {
  const v = (addr || "").trim();
  if (v.startsWith("::ffff:")) return v.slice(7);
  return v;
}

function isPrivateIpv4(ip: string): boolean {
  const parts = ip.split('.').map(Number);
  if (parts.length !== 4 || parts.some((n) => Number.isNaN(n) || n < 0 || n > 255)) return false;
  if (parts[0] === 10) return true;
  if (parts[0] === 192 && parts[1] === 168) return true;
  if (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) return true;
  return false;
}

function isPrivateIpv6(ip: string): boolean {
  const lower = ip.toLowerCase();
  return lower.startsWith('fc') || lower.startsWith('fd') || lower.startsWith('fe80:');
}

export function isTrustedLocalNetworkRequest(remoteAddr: string): boolean {
  const addr = normalize(remoteAddr);
  if (isLocalhostRequest(addr)) return true;
  const family = isIP(addr);
  if (family === 4) return isPrivateIpv4(addr);
  if (family === 6) return isPrivateIpv6(addr);
  return false;
}
