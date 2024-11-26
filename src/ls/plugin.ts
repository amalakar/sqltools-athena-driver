import { ILanguageServer, ILanguageServerPlugin } from '@sqltools/types';
import AthenaDriver from './driver';
import { DRIVER_ALIASES } from './../constants';

// Create a namespace to store the server instance
export namespace ServerStorage {
    let serverInstance: ILanguageServer | null = null;

    export function setServer(server: ILanguageServer) {
        serverInstance = server;
    }

    export function getServer(): ILanguageServer | null {
        return serverInstance;
    }
}

const YourDriverPlugin: ILanguageServerPlugin = {
  register(server) {
    DRIVER_ALIASES.forEach(({ value }) => {
      server.getContext().drivers.set(value, AthenaDriver as any);
    });
    ServerStorage.setServer(server);
  }
}

export default YourDriverPlugin;
