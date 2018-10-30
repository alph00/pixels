package cn.edu.ruc.iir.pixels.common.metadata;

import cn.edu.ruc.iir.pixels.common.exception.MetadataException;
import cn.edu.ruc.iir.pixels.common.utils.LogFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;


/**
 * instance of this class should not be reused.
 */
public class MetadataClientHandler extends ChannelInboundHandlerAdapter
{
    private ReqParams params;
    private final String token;
    private final MetadataClient client;

    public MetadataClientHandler(ReqParams params, String token, MetadataClient client)
    {
        this.params = params;
        this.token = token;
        this.client = client;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        ctx.writeAndFlush(params);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        try
        {
            LogFactory.Instance().getLog().info("client response: " + msg.toString());
            if (msg instanceof Object) {
                this.client.setResponse(token, msg);
            } else {
                // log the received params.
                LogFactory.Instance().getLog().info("Bad response, " + msg.toString());
                ctx.close();
            }
        }
        finally
        {
            ReferenceCountUtil.release(msg);
        }


    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception
    {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws MetadataException
    {
        LogFactory.Instance().getLog().error("error caught in metadata client.", e);
        ctx.close();
        throw new MetadataException("exception caught in MetadataClientHandler", e);
    }
}