type ParseOpts = {
    name?: string;
    types?: number[];
    text: string;
};
type ValueMapper = (param: any, index: number) => any;
type BindOpts = {
    portal?: string;
    binary?: boolean;
    statement?: string;
    values?: any[];
    valueMapper?: ValueMapper;
};
type ExecOpts = {
    portal?: string;
    rows?: number;
};
type PortalOpts = {
    type: 'S' | 'P';
    name?: string;
};
declare const serialize: {
    startup: (opts: Record<string, string>) => Buffer;
    password: (password: string) => Buffer;
    requestSsl: () => Buffer;
    sendSASLInitialResponseMessage: (mechanism: string, initialResponse: string) => Buffer;
    sendSCRAMClientFinalMessage: (additionalData: string) => Buffer;
    query: (text: string) => Buffer;
    parse: (query: ParseOpts) => Buffer;
    bind: (config?: BindOpts) => Buffer;
    execute: (config?: ExecOpts) => Buffer;
    describe: (msg: PortalOpts) => Buffer;
    close: (msg: PortalOpts) => Buffer;
    flush: () => Buffer<ArrayBufferLike>;
    sync: () => Buffer<ArrayBufferLike>;
    end: () => Buffer<ArrayBufferLike>;
    copyData: (chunk: Buffer) => Buffer;
    copyDone: () => Buffer<ArrayBufferLike>;
    copyFail: (message: string) => Buffer;
    cancel: (processID: number, secretKey: number) => Buffer;
};
export { serialize };
