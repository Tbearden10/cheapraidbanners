#!/usr/bin/env node
// Dev launcher:
// - Always starts a static server for ./public using `npx serve -s public -l 3000`.
// - If any args are provided, those args are forwarded to `wrangler dev`.
// - By default wrangler is started WITHOUT the `--assets` option so the static server
//   is the single source of frontend assets. If you explicitly include `--assets`
//   in the args, the script will append `--assets ./public` for wrangler.
// Examples:
//   npm run dev                        -> frontend only
//   npm run dev -- --env dev           -> frontend + wrangler dev (wrangler will NOT serve assets)
//   npm run dev -- --env dev --assets  -> frontend + wrangler dev AND wrangler will serve ./public

const { spawn } = require('child_process');

function spawnProcess(command, args, name) {
  const proc = spawn(command, args, { stdio: 'inherit', shell: true });
  proc.on('exit', (code, signal) => {
    if (signal) {
      console.log(`${name} exited with signal ${signal}`);
    } else {
      console.log(`${name} exited with code ${code}`);
    }
  });
  return proc;
}

const args = process.argv.slice(2);

// Start frontend server
console.log('Starting frontend server: serve -s public -l 3000');
const frontend = spawnProcess('npx', ['serve', '-s', 'public', '-l', '3000'], 'frontend');

if (args.length > 0) {
  // Build wrangler args: base = ['wrangler', 'dev', '--local']
  // Append --assets ./public only if user included the literal --assets in args.
  const includeAssets = args.includes('--assets');
  // Remove the literal --assets token before forwarding (we'll add path if present).
  const forwardedArgs = args.filter(a => a !== '--assets');

  const wranglerArgs = ['wrangler', 'dev', '--local', ...forwardedArgs];
  if (includeAssets) {
    wranglerArgs.push('--assets', './public');
  }

  console.log('Starting wrangler dev with args:', wranglerArgs.slice(2).join(' '));
  const wrangler = spawnProcess('npx', wranglerArgs, 'wrangler');

  const shutdown = () => {
    console.log('Shutting down child processes...');
    if (!frontend.killed) frontend.kill('SIGINT');
    if (!wrangler.killed) wrangler.kill('SIGINT');
    process.exit();
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  process.on('exit', shutdown);
} else {
  // Only frontend is running
  const shutdown = () => {
    console.log('Shutting down frontend server...');
    if (!frontend.killed) frontend.kill('SIGINT');
    process.exit();
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
  process.on('exit', shutdown);
}