class ElectronApi {
  private ipcRenderer = getIpcRenderer();

  constructor() {}

  send(msg, args = null) {
    this.ipcRenderer.send(msg, args);
  }

  async showOpenDialog(options) {
    const res = await this.ipcRenderer.invoke('showOpenDialog', options);
    return res;
  }

  async showSaveDialog(options) {
    const res = await this.ipcRenderer.invoke('showSaveDialog', options);
    return res;
  }

  async showItemInFolder(path) {
    const res = await this.ipcRenderer.invoke('showItemInFolder', path);
    return res;
  }

  async openExternal(url) {
    await this.ipcRenderer.invoke('openExternal', url);
  }

  async invoke(route, args) {
    const res = await this.ipcRenderer.invoke(route, args);
    return res;
  }

  addEventListener(channel: string, listener: Function) {
    this.ipcRenderer.on(channel, listener);
  }

  removeEventListener(channel: string, listener: Function) {
    this.ipcRenderer.removeListener(channel, listener);
  }
}

class TauriApi {
  private unlisteners = new Map<string, Array<{ listener: any; unlisten: Function }>>();

  constructor() {}

  async invoke<T = any>(route: string, args?: any): Promise<T> {
    const { invoke } = await import('@tauri-apps/api/core');
    try {
      return await invoke(route.replace(/-/g, '_'), args ?? {});
    } catch (err) {
      // Out-of-scope routes reject here; resolve with an errorMessage
      // envelope so errorValue loaders degrade to their designed fallback.
      const message = typeof err === 'string' ? err : err?.message ?? String(err);
      if (typeof message === 'string' && message.includes('Route not implemented')) {
        return { errorMessage: message } as T;
      }
      throw err;
    }
  }

  async send(msg: string, args: any = null) {
    return Promise.resolve();
  }

  async addEventListener(channel: string, listener: any) {
    const { listen } = await import('@tauri-apps/api/event');
    const unlisten = await listen(channel, event => {
      listener(null, event.payload);
    });
    const entries = this.unlisteners.get(channel) || [];
    entries.push({ listener, unlisten });
    this.unlisteners.set(channel, entries);
  }

  async removeEventListener(channel: string, listener: any) {
    const entries = this.unlisteners.get(channel) || [];
    for (const entry of entries) {
      if (entry.listener === listener) {
        entry.unlisten();
      }
    }
    this.unlisteners.set(
      channel,
      entries.filter(entry => entry.listener !== listener)
    );
    if (this.unlisteners.get(channel)?.length === 0) {
      this.unlisteners.delete(channel);
    }
  }

  async showOpenDialog(options: any) {
    return null;
  }

  async showSaveDialog(options: any) {
    return null;
  }

  async showItemInFolder(path: string) {}

  async openExternal(url: string) {}
}

function getIpcRenderer() {
  if (window['require']) {
    const electron = window['require']('electron');
    return electron?.ipcRenderer;
  }
  return null;
}

function isTauriAvailable() {
  return !!(window as any).__TAURI__ || !!(window as any).__TAURI_INTERNALS__;
}

export function isElectronAvailable() {
  return !!(getIpcRenderer() || isTauriAvailable());
}

const apiInstance = getIpcRenderer() ? new ElectronApi() : isTauriAvailable() ? new TauriApi() : null;

export default function getElectron(): ElectronApi | TauriApi {
  return apiInstance;
}