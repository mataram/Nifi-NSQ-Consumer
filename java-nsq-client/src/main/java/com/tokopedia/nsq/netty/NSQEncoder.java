package com.tokopedia.nsq.netty;

import com.tokopedia.nsq.NSQCommand;
import com.tokopedia.nsq.NSQCommand;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public class NSQEncoder extends MessageToMessageEncoder<NSQCommand> {

	@Override
	protected void encode(ChannelHandlerContext ctx, NSQCommand message, List<Object> out) throws Exception {
		ByteBuf buf = Unpooled.buffer();
		buf.writeBytes(message.getLine().getBytes("utf8"));
		
		//for MPUB messages.
		if (message.getData().size() > 1) {
			//write total bodysize and message size
			int bodySize = 4; //4 for total messages int.
			for (byte[] data : message.getData()) {
				bodySize += 4; //message size
				bodySize += data.length;
			}
			buf.writeInt(bodySize);
			buf.writeInt(message.getData().size());
		}
		
		for (byte[] data : message.getData()) {
			buf.writeInt(data.length);
			buf.writeBytes(data);
		}
		out.add(buf);
	}
}
