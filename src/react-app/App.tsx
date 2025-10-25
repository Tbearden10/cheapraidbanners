import { useEffect, useState } from "react";
import reactLogo from "./assets/react.svg";
import viteLogo from "/vite.svg";
import cloudflareLogo from "./assets/Cloudflare_Logo.svg";
import honoLogo from "./assets/hono.svg";
import "./App.css";

function App() {
  const [online, setOnline] = useState<number | null>(null);

  useEffect(() => {
    const connect = () => {
      const wsProtocol = location.protocol === "https:" ? "wss:" : "ws:";
      const ws = new WebSocket(`${wsProtocol}//${location.host}/api/live`);

      ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.online !== undefined) setOnline(data.online);
      };

      ws.onclose = () => {
        // reconnect after delay
        setTimeout(connect, 3000);
      };
    };

    connect();
  }, []);

  return (
    <div className="container">
      <div className="logos">
        <a href="https://vite.dev" target="_blank">
          <img src={viteLogo} className="logo" alt="Vite logo" />
        </a>
        <a href="https://react.dev" target="_blank">
          <img src={reactLogo} className="logo react" alt="React logo" />
        </a>
        <a href="https://hono.dev/" target="_blank">
          <img src={honoLogo} className="logo cloudflare" alt="Hono logo" />
        </a>
        <a href="https://workers.cloudflare.com/" target="_blank">
          <img
            src={cloudflareLogo}
            className="logo cloudflare"
            alt="Cloudflare logo"
          />
        </a>
      </div>

      <h1 className="site-title">cheapraidbanners.com</h1>

      <div className="card">
        <p className="text-xl">Live Visitors: {online ?? "..."}</p>
      </div>

      <p className="read-the-docs">Built with Cloudflare + Hono + React + Vite</p>
    </div>
  );
}

export default App;
