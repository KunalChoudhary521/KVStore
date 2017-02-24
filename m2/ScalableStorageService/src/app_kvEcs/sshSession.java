package app_kvEcs;

import com.jcraft.jsch.*;
import java.io.*;

public class sshSession
{
    public Session session;
    public Channel channel;

    sshSession()
    {
        session = null;
        channel = null;
    }

    public void connectSsh(String user, String host, int port)
    {
        try
        {
            JSch ssh = new JSch();
            String privateKey = "id_rsa";
            String knowHostFile = "known_hosts";
            this.session = null;

            ssh.setKnownHosts(knowHostFile);
            this.session = ssh.getSession(user, host, port);
            ssh.addIdentity(privateKey);

            this.session.connect(10000);//program ends if client doesn't connect in this many milliseconds
            System.out.println("Connected to " + user + "@" + host + ":" + port);

            Thread.sleep(3* 1000);
        }
        catch(Exception ex)
        {
            System.out.println("Failed to connect to " + user + "@" + host + ":" + port);
            ex.printStackTrace();
        }
    }
    public void runServer(String dir, String command)
    {
        try
        {
            this.channel = this.session.openChannel("shell");
            OutputStream chOut = this.channel.getOutputStream();
            PrintStream sendCmds = new PrintStream(chOut, true);
            this.channel.setOutputStream(System.out, true);
            this.channel.connect(4 * 1000);//server process begins

            sendCmds.println(dir);
            sendCmds.println(command + " &");

            sendCmds.close();

            do {
                Thread.sleep(3 *1000);
            }while(this.channel.isEOF());//wait for channel to to be empty

        }
        catch(Exception ex)
        {
            System.out.println("Failed to execute over ssh");
            ex.printStackTrace();
        }
        return;
    }
    public void run1Server(String command)//only used to run 1 server
    {
        try
        {
            this.channel = this.session.openChannel("exec");
            ((ChannelExec) this.channel).setPty(true);
            ((ChannelExec) this.channel).setCommand(command);
            this.channel.setInputStream(null);
            ((ChannelExec) this.channel).setErrStream(System.err);

            this.channel.connect();//server process begins

        }
        catch(Exception ex)
        {
            System.out.println("Failed to execute over ssh");
            System.out.println("Command: " + command);
            ex.printStackTrace();
        }
    }
    public void stopAllServer()//does not currently work
    {
        try
        {
            this.channel = this.session.openChannel("shell");
            OutputStream chOut = this.channel.getOutputStream();
            PrintStream sendCmds = new PrintStream(chOut, true);
            this.channel.setOutputStream(System.out, true);

            this.channel.connect(4 * 1000);

            sendCmds.println("ps -ef | grep java | grep choudh65 | awk '{print $2}'");

        }
        catch (Exception ex)
        {
            System.out.println("Failed to stop server");
            ex.printStackTrace();
        }

        this.channel.disconnect();
    }
    public void readSshInput(InputStream in) throws Exception
    {
        byte[] tmp = new byte[1024];
        while(true)
        {
            while(in.available() > 0)
            {
                int i = in.read(tmp, 0 ,1024);
                if(i < 0)
                {
                    break;
                }
                System.out.print(new String(tmp,0,i));
            }

            if(this.channel.isClosed())
            {
                if(in.available() > 0)
                {
                    continue;
                }
                System.out.println("Read Ended: " + this.channel.getExitStatus());
                break;
            }
        }
    }
}
