using System;
using System.Collections;
using System.Collections.Generic;
using System.Net;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Fakes.models;

namespace RabbitMQ.Fakes
{
    public class FakeConnection : IConnection
    {
        private readonly RabbitServer _server;

        public FakeConnection(RabbitServer server)
        {
            _server = server;
            Models = new List<FakeModel>();
        }

        public List<FakeModel> Models { get; private set; }

        public EndPoint LocalEndPoint { get; set; }

        public EndPoint RemoteEndPoint { get; set; }

        public int LocalPort { get; set; }

        public int RemotePort { get; set; }

        public void Dispose()
        {
            
        }

        public IModel CreateModel()
        {
            var model = new FakeModel(_server);
            Models.Add(model);

            return model;
        }

        public void Close()
        {
            Close(1,null,0);
        }

        public void Close(ushort reasonCode, string reasonText)
        {
            Close(reasonCode,reasonText,0);
        }

        public void Close(int timeout)
        {
            Close(1,null,timeout);
        }

        public void Close(ushort reasonCode, string reasonText, int timeout)
        {
            IsOpen = false;
            CloseReason = new ShutdownEventArgs(ShutdownInitiator.Library, reasonCode, reasonText);

            Models.ForEach(m=>m.Close());
        }

        public void Abort()
        {
            Abort(1, null, 0);
        }

        public void Abort(int timeout)
        {
           Abort(1,null,timeout);
        }

        public void Abort(ushort reasonCode, string reasonText)
        {
            Abort(reasonCode, reasonText, 0);
        }
        public void Abort(ushort reasonCode, string reasonText, int timeout)
        {
            IsOpen = false;
            CloseReason = new ShutdownEventArgs(ShutdownInitiator.Library,reasonCode,reasonText );

            this.Models.ForEach(m=>m.Abort());
        }

        public void HandleConnectionBlocked(string reason)
        {
            
        }

        public void HandleConnectionUnblocked()
        {
            
        }

        public AmqpTcpEndpoint Endpoint { get; set; }

        public IProtocol Protocol { get; set; }

        IDictionary<string, object> IConnection.ServerProperties
        {
            get { throw new NotImplementedException(); }
        }

        IList<ShutdownReportEntry> IConnection.ShutdownReport
        {
            get { throw new NotImplementedException(); }
        }

        public string ClientProvidedName { get; }
        public ConsumerWorkService ConsumerWorkService { get; }
        event EventHandler<CallbackExceptionEventArgs> IConnection.CallbackException
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event EventHandler<EventArgs> RecoverySucceeded;
        public event EventHandler<ConnectionRecoveryErrorEventArgs> ConnectionRecoveryError;

        public event EventHandler<ConnectionBlockedEventArgs> ConnectionBlocked;
        event EventHandler<ShutdownEventArgs> IConnection.ConnectionShutdown
        {
            add { throw new NotImplementedException(); }
            remove { throw new NotImplementedException(); }
        }

        public event EventHandler<EventArgs> ConnectionUnblocked;

        public ushort ChannelMax { get; set; }

        IDictionary<string, object> IConnection.ClientProperties
        {
            get { throw new NotImplementedException(); }
        }

        public uint FrameMax { get; set; }

        public ushort Heartbeat { get; set; }

        public IDictionary ClientProperties { get; set; }

        public IDictionary ServerProperties { get; set; }

        public AmqpTcpEndpoint[] KnownHosts { get; set; }

        public ShutdownEventArgs CloseReason { get; set; }

        public bool IsOpen { get; set; }

        public bool AutoClose { get; set; }

        public IList ShutdownReport { get; set; }
    }
}