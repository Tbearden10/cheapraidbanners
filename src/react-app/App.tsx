import { useState, useEffect } from "react";
import reactLogo from "./assets/react.svg";
import viteLogo from "/vite.svg";
import cloudflareLogo from "./assets/Cloudflare_Logo.svg";
import honoLogo from "./assets/hono.svg";
import "./App.css";

function App() {
  const [online, setOnline] = useState<number | null>(null);

  useEffect(() => {
    // WebSocket connection to DO
    const ws = new WebSocket(
      `${location.origin.replace(/^http/, "ws")}/api/live`
    );

    ws.onopen = () => console.log("Connected to LiveUsers WebSocket");

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        setOnline(data.online);
      } catch {}
    };

    ws.onclose = () => console.log("LiveUsers WebSocket closed");

    return () => ws.close();
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
        <button onClick={() => alert("Soon™")} aria-label="placeholder-button">
          Coming Soon
        </button>
      </div>

      {/* Live visitor tracker */}
      <div
        style={{
          position: "absolute",
          bottom: "10px",
          right: "10px",
          fontSize: "0.8rem",
          color: "#888",
          opacity: 0.8,
        }}
      >
        {online !== null ? `Live visitors: ${online}` : "Connecting..."}
      </div>

      <p className="read-the-docs">
        Built with Cloudflare + Hono + React + Vite
      </p>
    </div>
  );
}

export default App;
