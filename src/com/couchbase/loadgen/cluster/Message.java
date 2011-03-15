package com.couchbase.loadgen.cluster;

public class Message {
	public static final byte MAGIC = (byte) 60;
	public static final byte OP_CONFIG = (byte) 11;
	public static final byte OP_STATS = (byte) 13;
	public static final byte OP_START = (byte) 15;
	public static final byte OP_FINISH = (byte) 17;
	public static final byte OP_STOP = (byte) 19;
	
	private static final int MAGIC_OFFSET = 0;
	private static final int OPCODE_OFFSET = 1;
	private static final int LENGTH_OFFSET = 3;
	private static final int LENGTH_LENGTH = 8;
	private static final int BODY_OFFSET = 11;
	
	private byte magic;
	private byte opcode;
	private byte[] length;
	private byte[] body;
	
	public Message() {
		magic = MAGIC;
		length = new byte[LENGTH_LENGTH];
		body = new byte[0];
	}
	
	public byte getMagic() {
		return magic;
	}
	
	public void setMagic(byte magic) {
		this.magic = magic;
	}
	
	public byte getOpcode() {
		return opcode;
	}
	
	public void setOpcode(byte opcode) {
		this.opcode = opcode;
	}
	
	public int getLength() {
		int len = 0;
		for (int i = 0; i < LENGTH_LENGTH; i++)
			len += (int)Math.pow(2.0, (double)(LENGTH_LENGTH - i - 1)) * length[i];
		return len;
	}
	
	private void setLength(int len) {
		for (int i = 0; i < LENGTH_LENGTH; i++) {
			length[i] = (byte)(len / (int)Math.pow(2.0, (double)(LENGTH_LENGTH - i - 1)));
			len = (len % (int)Math.pow(2.0, (double)(LENGTH_LENGTH - i - 1)));
		}
	}
	
	public byte[] getBody() {
		return this.body;
	}
	
	public void setBody(byte[] body) {
		setLength(body.length);
		this.body = body;
	}
	
	public byte[] encode() {
		byte[] buffer = new byte[BODY_OFFSET + (int)getLength()];
		buffer[MAGIC_OFFSET] = magic;
		buffer[OPCODE_OFFSET] = opcode;
		
		for (int i = 0; i < LENGTH_LENGTH; i++)
			buffer[LENGTH_OFFSET + i] = length[i];
		
		for (int i = 0; i < body.length; i++)
			buffer[BODY_OFFSET + i] = body[i];
		
		return buffer;
	}
	
	public static Message decode(byte[] buffer) {
		byte[] body;
		try {
			Message message = new Message();
			message.setMagic(buffer[MAGIC_OFFSET]);
			if (message.getMagic() != MAGIC) {
				return null;
			}
			message.setOpcode(buffer[OPCODE_OFFSET]);
			
			int len = 0;
			for (int i = 0; i < LENGTH_LENGTH; i++) {
				len += (int)Math.pow(2.0, (double)(LENGTH_LENGTH - i - 1)) * buffer[i + LENGTH_OFFSET];
			}
			
			body = new byte[len];
			for (int i = 0; i < len; i++) {
				body[i] = buffer[i + BODY_OFFSET];
			}
			message.setBody(body);	
			return message;
		} catch (Exception e) {
			return null;
		}
	}
}
