import { TransformOptions } from 'stream';
import { Mode, BackendMessage } from './messages';
export type Packet = {
    code: number;
    packet: Buffer;
};
type StreamOptions = TransformOptions & {
    mode: Mode;
};
export type MessageCallback = (msg: BackendMessage) => void;
export declare class Parser {
    private buffer;
    private bufferLength;
    private bufferOffset;
    private reader;
    private mode;
    constructor(opts?: StreamOptions);
    parse(buffer: Buffer, callback: MessageCallback): void;
    private mergeBuffer;
    private handlePacket;
}
export {};
