import { spawn } from 'node:child_process';
import { setTimeout } from 'node:timers/promises';
import { createClient } from './client';
import { saveProfile } from './config';

export const login = async (apiUrl: string, openBrowser = true) => {
  const client = createClient(apiUrl);
  const request = await client.cliAuth.startLogin.mutate();
  console.log(`Open ${request.verificationUrl}\nConfirm this code: ${request.userCode}\nWaiting for browser approval…`);
  if (openBrowser) launchBrowser(request.verificationUrl);
  const controller = new AbortController();
  const cancel = () => controller.abort();
  process.once('SIGINT', cancel);
  process.once('SIGTERM', cancel);
  try {
    while (Date.now() < request.expiresAt.getTime()) {
      await setTimeout(request.intervalSeconds * 1000, undefined, { signal: controller.signal });
      const result = await client.cliAuth.pollLogin.mutate({ deviceCode: request.deviceCode });
      if (result.status !== 'approved') continue;
      await saveProfile(apiUrl, { token: result.token, expiresAt: result.expiresAt.toISOString() });
      console.log('Signed in. Run pgstack auth status to see your organizations.');
      return;
    }
    throw new Error('Login timed out. Run pgstack auth login again.');
  } finally {
    process.off('SIGINT', cancel);
    process.off('SIGTERM', cancel);
  }
};

const launchBrowser = (url: string) => {
  const executable = process.platform === 'darwin' ? 'open' : process.platform === 'win32' ? 'rundll32' : 'xdg-open';
  const args = process.platform === 'win32' ? ['url.dll,FileProtocolHandler', url] : [url];
  const child = spawn(executable, args, { detached: true, stdio: 'ignore' });
  child.on('error', () => console.log('Open the URL above in your browser to continue.'));
  child.unref();
};
