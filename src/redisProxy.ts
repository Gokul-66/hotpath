import net from "node:net";

/** Sits between a cache and Redis, so tests and the demo can cut the connection or freeze it. */
export class RedisProxy {
  private readonly server: net.Server;
  private readonly sockets = new Set<net.Socket>();
  private mode: "pass" | "freeze" | "down" = "pass";

  constructor(private readonly target: string) {
    const { hostname, port } = new URL(target);
    this.server = net.createServer((client) => {
      if (this.mode === "down") return client.destroy();
      const upstream = net.connect(Number(port || 6379), hostname);
      for (const s of [client, upstream]) {
        this.sockets.add(s);
        s.on("error", () => {});
        s.on("close", () => {
          this.sockets.delete(s);
          client.destroy();
          upstream.destroy();
        });
      }
      client.on("data", (d) => this.mode !== "freeze" && upstream.write(d));
      upstream.on("data", (d) => this.mode !== "freeze" && client.write(d));
    });
  }

  /** The proxy's URL, with the target's credentials. */
  async start(): Promise<string> {
    await new Promise<void>((r) => this.server.listen(0, "127.0.0.1", r));
    const url = new URL(this.target);
    url.hostname = "127.0.0.1";
    url.port = String((this.server.address() as net.AddressInfo).port);
    return url.toString();
  }

  /** Connections stay open but nothing gets through, like a hung server. */
  freeze(): void {
    this.mode = "freeze";
  }

  /** Connections drop and new ones are refused. */
  cut(): void {
    this.mode = "down";
    for (const s of this.sockets) s.destroy();
  }

  restore(): void {
    this.mode = "pass";
    // Frozen connections lost bytes, so they cannot be reused.
    for (const s of this.sockets) s.destroy();
  }

  async stop(): Promise<void> {
    for (const s of this.sockets) s.destroy();
    await new Promise((r) => this.server.close(r));
  }
}
