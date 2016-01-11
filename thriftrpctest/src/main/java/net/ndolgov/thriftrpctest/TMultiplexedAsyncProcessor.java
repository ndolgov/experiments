package net.ndolgov.thriftrpctest;

import org.apache.thrift.AsyncProcessFunction;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TBaseAsyncProcessor;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.server.AbstractNonblockingServer;

import java.util.HashMap;
import java.util.Map;

/**
 * Very much of a hack required because of the internal structure of TBaseAsyncProcessor in 0.9.2. Based on
 * TMultiplexedProcessor. Requires TMultiplexedProtocol on the client side. <P>
 *
 * <b>Please see https://issues.apache.org/jira/browse/THRIFT-2427 for a more permanent solution that will hopefully
 * be released one day in the official libthrift distribution.</b>
 */
public final class TMultiplexedAsyncProcessor extends TBaseAsyncProcessor {
    private final Map<String, TBaseAsyncProcessor> nameToProcessor = new HashMap<>();
    private final Map<String, Object> nameToIface = new HashMap<>();

    public TMultiplexedAsyncProcessor() {
        super(null, null);
    }

    public void registerProcessor(String serviceName, TBaseAsyncProcessor processor, Object iface) {
        this.nameToProcessor.put(serviceName, processor);
        this.nameToIface.put(serviceName, iface);
    }

    public boolean process(final AbstractNonblockingServer.AsyncFrameBuffer fb) throws TException {

        final TProtocol in = fb.getInputProtocol();
        final TProtocol out = fb.getOutputProtocol();

        //Find processing function
        final TMessage msg = in.readMessageBegin();


        if (msg.type != TMessageType.CALL && msg.type != TMessageType.ONEWAY) {
            throw new TException("This should not have happened!?");
        }
        final String serviceName = serviceName(msg.name);
        final Object iface = iface(serviceName);
        final AsyncProcessFunction fn = asyncProcessFunction(msg.name, serviceName);

        if (fn == null) {
            TProtocolUtil.skip(in, TType.STRUCT);
            in.readMessageEnd();
            TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '"+msg.name+"'");
            out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
            x.write(out);
            out.writeMessageEnd();
            out.getTransport().flush();
            fb.responseReady();
            return true;
        }

        //Get Args
        TBase args = (TBase)fn.getEmptyArgsInstance();

        try {
            args.read(in);
        } catch (TProtocolException e) {
            in.readMessageEnd();
            TApplicationException x = new TApplicationException(TApplicationException.PROTOCOL_ERROR, e.getMessage());
            out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
            x.write(out);
            out.writeMessageEnd();
            out.getTransport().flush();
            fb.responseReady();
            return true;
        }
        in.readMessageEnd();


        //start off processing function
        fn.start(iface, args,fn.getResultHandler(fb,msg.seqid));
        return true;
    }

    private Object iface(String serviceName) throws TException {
        final Object iface = nameToIface.get(serviceName);
        if (iface == null) {
            throw new TException("Service iface not found: " + serviceName);
        }
        return iface;
    }

    private AsyncProcessFunction asyncProcessFunction(String msgName, String serviceName) throws TException {
        final TBaseAsyncProcessor actualProcessor = nameToProcessor.get(serviceName);
        if (actualProcessor == null) {
            throw new TException("Service name not found: " + serviceName + ".  Did you forget to call registerProcessor()?");
        }
        final String fnName = msgName.substring(serviceName.length() + TMultiplexedProtocol.SEPARATOR.length());

        return (AsyncProcessFunction) actualProcessor.getProcessMapView().get(fnName);
    }

    private String serviceName(String name) throws TException {
        // Extract the service name
        final int index = name.indexOf(TMultiplexedProtocol.SEPARATOR);
        if (index < 0) {
            throw new TException("Service name not found in msg name: " + name + ".  Did you forget to use a TMultiplexProtocol in your client?");
        }

        // Create a new TMessage, something that can be consumed by any TProtocol
        return name.substring(0, index);
    }

    @Override
    public boolean process(TProtocol tProtocol, TProtocol tProtocol1) throws TException {
        return false;
    }
}
