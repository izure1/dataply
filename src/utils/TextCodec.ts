export class TextCodec {
  protected static readonly TextEncoder = new TextEncoder()
  protected static readonly TextDecoder = new TextDecoder()

  static GetEncoder(): TextEncoder {
    return TextCodec.TextEncoder
  }

  static getDecoder(): TextDecoder {
    return TextCodec.TextDecoder
  }

  protected getEncoder(): TextEncoder {
    return TextCodec.GetEncoder()
  }

  protected getDecoder(): TextDecoder {
    return TextCodec.getDecoder()
  }

  encode(text: string): Uint8Array {
    const encoder = this.getEncoder()
    return encoder.encode(text)
  }

  decode(source: Uint8Array): string {
    const decoder = this.getDecoder()
    return decoder.decode(source)
  }
}
