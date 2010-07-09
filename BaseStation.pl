use IO::Socket;
use IO::Select;
#use Net::hostent;
use strict;


my $VERSION = '1.2b';


my @defaultParams = (1429, 1427, 1425);
my @paramDef = qw(SENSOR_PORT SUBSCRIBER_PORT LOOPBACK_PORT);


my @params = @ARGV;
foreach (@params)
{
   if (m/^\s*$/i || m/\s/i)
   {
      $_ = sprintf('"%s"', $_);
   }
}


printf '
BaseStation.pl V%s

    USAGE:
        perl BaseStation.pl %s

    EXAMPLE(&defaults):
        perl BaseStation.pl %s

    Envoked:
        perl BaseStation.pl %s

', $VERSION, sprintf('<%s>', join('> <', @paramDef)), join(' ', @defaultParams), join(' ', @params);


my %p;
@p{@paramDef} = @defaultParams;

while ($_ = shift(@paramDef))
{
   my $param = shift(@ARGV);
   last if (! defined $param);
   next if (!$param);
   $p{$_} = $param;
}



my %portConfigBasis = (
   Proto     => 'tcp',
   LocalPort => undef,
   Listen    => SOMAXCONN,
   Reuse     => 1
);



# Clear non-essential data space

undef(@defaultParams);
undef(@paramDef);
undef(@params);
undef($VERSION);




# lets reserve main program dataspace

# because external IDs will never have a newLine character in them, lets include a single one in internal ones

my $RESTART_SENSOR_CMD = "0\n";
my $RESTART_SUBSCRIBER_CMD = "1\n";
my $SENSOR_SERVER_ID = "\nSENSOR_SERVER";
my $SUBSCRIBER_SERVER_ID = "\nSUBSCRIBER_SERVER";
my $SUBSCRIBER_ID = "\nSUBSCRIBER";
my $LOOPBACK_IN_ID = "\nLOOPBACK_IN";
my $LOOPBACK_OUT_ID = "\nLOOPBACK_OUT";
my $recoveryTimeout = 2; # seconds
my $loopbackTestTimeout = 5; # seconds


my %SENSOR_SERVER_SOCKET_CONFIG = %portConfigBasis;
$SENSOR_SERVER_SOCKET_CONFIG{LocalPort} = $p{SENSOR_PORT};

my %SUBSCRIBER_SERVER_SOCKET_CONFIG = %portConfigBasis;
$SUBSCRIBER_SERVER_SOCKET_CONFIG{LocalPort} = $p{SUBSCRIBER_PORT};

my %CMDsToConfigs = (
   $RESTART_SENSOR_CMD => \%SENSOR_SERVER_SOCKET_CONFIG,
   $RESTART_SUBSCRIBER_CMD => \%SUBSCRIBER_SERVER_SOCKET_CONFIG
);
      

my %CMDsToIDs = (
   $RESTART_SENSOR_CMD => $SENSOR_SERVER_ID,
   $RESTART_SUBSCRIBER_CMD => $SUBSCRIBER_SERVER_ID
);


# List of registerable function pointers
# coalescing to static handles is also valid
my $SENSOR_HANDLE = \&handleSensor;
my $CRITICAL_HANDLE = \&handleCritical;
my $SUBSCRIBER_HANDLE = \&handleSubscriber;
my $SENSOR_SERVER_HANDLE = \&handleSensorServer;
my $SUBSCRIBER_SERVER_HANDLE = \&handleSubscriberServer;


# Data Elelemts that should be dumped on error
# and then re-instantiated
my $inputSelector;
my $LOOPBACK_OUT;
my $SUBSCRIBER_SOCKET;
my $SENSOR_SERVER_SOCKET;
my $SUBSCRIBER_SERVER_SOCKET;
my %StatsByIDs;
my %IDsToSockets;
my %SocketsToIDs;
my %SocketsToHandles;



# All basis material is gathered and the main entry point for the program is read to run

myLog("Server is running!\nSENSOR PORT:     $p{SENSOR_PORT}\nSUBSCRIBER PORT: $p{SUBSCRIBER_PORT}\nLOOPBACK PORT:   $p{LOOPBACK_PORT}");

while (1)
{
   print "\n\n\n";
   # This eval statement should never break
   # If it ever does, we will become re-entrant,
   # hence the wrapping while(1) call to re-init
   eval
   {
      
      
      # > Begin init sequence
      
      
      
      $SENSOR_SERVER_SOCKET = IO::Socket::INET->new(%SENSOR_SERVER_SOCKET_CONFIG) || die sprintf('Failed to start server application on port %s', $p{SENSOR_PORT});
      $SUBSCRIBER_SERVER_SOCKET = IO::Socket::INET->new(%SUBSCRIBER_SERVER_SOCKET_CONFIG) || die sprintf('Failed to start server application on port %s', $p{SUBSCRIBER_PORT});
      
      
      my %LOOPBACK_SERVER_SOCKET_CONFIG = %portConfigBasis;
      $LOOPBACK_SERVER_SOCKET_CONFIG{LocalPort} = $p{LOOPBACK_PORT};
      my $LOOPBACK_SERVER_SOCKET = IO::Socket::INET->new(%LOOPBACK_SERVER_SOCKET_CONFIG) || die sprintf('Failed to start server application on port %s', $p{LOOPBACK_PORT});
      undef(%LOOPBACK_SERVER_SOCKET_CONFIG);
      
      
      $inputSelector = IO::Select->new() || die 'Failed to construct IO handler';
      
      
      
      my $LOOPBACK_IN = IO::Socket::INET->new(Proto => 'tcp', PeerAddr => '127.0.0.1', PeerPort => $p{LOOPBACK_PORT}) || die 'Failed to establish LOOPBACK_IN';
      $LOOPBACK_OUT = $LOOPBACK_SERVER_SOCKET->accept() || die 'Failed to establish LOOPBACK_OUT';
      $LOOPBACK_SERVER_SOCKET->close() || die 'Failed to release LOOPBACK_SERVER_SOCKET';
      undef($LOOPBACK_SERVER_SOCKET);
      
      
      # >> Begin Loopback Test
      
      
      $inputSelector->add($LOOPBACK_IN) || die sprintf('Failed to add %s to handler', 'LOOPBACK_IN');
      (print $LOOPBACK_OUT "INIT\n") || die 'Failed to send on LOOPBACK';
      ($_) = $inputSelector->can_read($loopbackTestTimeout);
      die 'LOOPBACK PORT SOCKET UNRESPONSIVE' if !$_;
      # Lets trash the init send action
      <$_>;
      undef($_);
      
      # <<  End Loopback Test
      
      
      coalesce($SENSOR_SERVER_SOCKET, $SENSOR_SERVER_ID, $SENSOR_SERVER_HANDLE) || die 'Failed to coalesce critical socket';
      coalesce($SUBSCRIBER_SERVER_SOCKET, $SUBSCRIBER_SERVER_ID, $SUBSCRIBER_SERVER_HANDLE) || die 'Failed to coalesce critical socket';
      coalesce($LOOPBACK_IN, $LOOPBACK_IN_ID, $CRITICAL_HANDLE) || die 'Failed to coalesce critical socket';
      coalesce($LOOPBACK_OUT, $LOOPBACK_OUT_ID, $CRITICAL_HANDLE) || die 'Failed to coalesce critical socket';
      $inputSelector->add($SENSOR_SERVER_SOCKET) || die sprintf('Failed to add %s to handler', 'sensor_server_socket');
      $inputSelector->add($SUBSCRIBER_SERVER_SOCKET) || die sprintf('Failed to add %s to handler', 'subscriber_server_socket');
      $inputSelector->add($LOOPBACK_OUT) || die sprintf('Failed to add %s to handler', 'LOOPBACK_OUT');
      
      
      # < End init sequence
      
      
      while (1) # Dependencies remain intact and functional
      {
		 # This is not busy wait, this is a thread blocking call
         foreach ($inputSelector->can_read())
         {
            $SocketsToHandles{$_}->($_);
			# get registered function handle for the socket
			# then dereference and pass the socket to the
			# function.
         }
      }
      
      
   };
   if ($@)
   {
      myLog($@ . "\nA major error occured and broke the runtime!\n\nAttempting to recover in $recoveryTimeout seconds ...");
      
   };
   
   undef(%StatsByIDs);
   undef(%IDsToSockets);
   undef(%SocketsToIDs);
   undef(%SocketsToHandles);
   undef($inputSelector);
   undef($SUBSCRIBER_SOCKET);
   undef($LOOPBACK_OUT);
   undef($SENSOR_SERVER_SOCKET);
   undef($SUBSCRIBER_SERVER_SOCKET);
   
   sleep($recoveryTimeout);
}


###################################
#                                 #
#  Begin Function & Handle space  #
#                                 #
###################################



# Establishes new sensor connections
# that are pending identification
# OR initiates a socket recovery over the
# loopback for the connection server
sub handleSensorServer
{
   my $serverSocket = shift;
   # Establish a new sensor socket
   if (register($serverSocket, $SENSOR_HANDLE))
   {
      myLog('A SENSOR has connected');
   }
   else
   {
      myLog('Something may be wrong with the SENSOR_SERVER_PORT...');
      (print $LOOPBACK_OUT $RESTART_SENSOR_CMD) || die 'Failed to send on LOOPBACK';
   }
}


# Establishes new single instance
# subscriber connection. Any overlap
# in connection requests will result
# in clobbering. Will also initiate a
# socket recovery over the loopback if
# an error occurs with the server socket.
sub handleSubscriberServer
{
   my $serverSocket = shift;
   # Attempting to set a new subscriber
   if ($SUBSCRIBER_SOCKET)
   {
      # We already have an active subscriber...
      myLog('Dropping existing subscriber...');
      unregister($SUBSCRIBER_SOCKET, $SUBSCRIBER_ID);
   }
   if($SUBSCRIBER_SOCKET = register($serverSocket, $SUBSCRIBER_HANDLE, $SUBSCRIBER_ID))
   {
      myLog('SUBSCRIBER has connected.');
   }
   else
   {
      myLog('Something may be wrong with the SUBSCRIBER_SERVER_PORT...');
      (print $LOOPBACK_OUT $RESTART_SUBSCRIBER_CMD) || die 'Failed to send on LOOPBACK';
   }
}


# Allows for a message relay system
# between a subscriber and all sensors
# keyed by MAC|<PAYLOAD>
sub handleSubscriber
{
   my $srcSocket = shift;
   # We are being asked to foward a message
   my ($buff, $err) = readLine($srcSocket);
   if ($err)
   {
      # Disconnected
      unregister($srcSocket, $SUBSCRIBER_ID);
      $SUBSCRIBER_SOCKET = undef;
   }
   elsif ($buff =~ m/^([^|]+)[|]/i)
   {
      my ($ID, $MSG) = ($1, $');
      if ($MSG && $MSG !~ m/^\s*$/i)
      {
         my $dstSocket = $IDsToSockets{$ID};
         if ($dstSocket)
         {
            sendOrKill($dstSocket, $MSG, $ID);
         }
         else
         {
            if (exists $IDsToSockets{$ID})
            {
               myLog("$ID is disconnected");
            }
            else
            {
               myLog("$ID has never been seen");
            }
            sendOrKill($srcSocket,  "E|$ID|1\n", $SUBSCRIBER_ID);
         }
      }
      else
      {
         chomp($buff);
         myLog("SUBSCRIBER sent a null message \"$buff\"");
      }
   }
   else
   {
      chomp($buff);
      myLog("Could not make sense of \"$buff\" from SUBSCRIBER");
   }

}



# Fowards messages or
# terminates connections with
# sensors based on readability
sub handleSensor
{
   my $socket = shift;
   my ($buff, $err) = readLine($socket);
   if ($err)
   {
      # Connection lost to host
      unregister($socket);
   }
   elsif ($buff =~ m/^[^|]*[|][^|]*[|]([^|]+)[|]?.*\n$/i)
   {
      if (my $status = coalesce($socket, $1))
      {
         if ($status == 1)
         {
            chomp($buff);
            myLog("Message \"$buff\" was dropped as an INIT message");
         }
         elsif ($SUBSCRIBER_SOCKET)
         {
            sendOrKill($SUBSCRIBER_SOCKET, $buff, $SUBSCRIBER_ID);
         }
         else
         {
            chomp($buff);
            myLog("Message \"$buff\" was dropped because there is no SUBSCRIBER");
         }
      }
      else
      {
         chomp($buff);
         myLog("Message \"$buff\" was dropped because of a fault");
      }
   }
   else
   {
      chomp($buff);
      my $src = $SocketsToIDs{$socket} || '<unverified>';
      myLog("Could not make sense of \"$buff\" from SENSOR $src");
   }
}


# Attempt to send a message
# If the socket dies, we simply
# disconnect it.
sub sendOrKill
{
   my $socket = shift;
   my $MSG = shift;
   my $ID = shift || $SocketsToIDs{$socket}; # should never be unverified
   my $rval = 0;
   
   if (print $socket $MSG)
   {
      $rval = 1;
   }
   else
   {
      myLog("Failed to send \"$MSG\" to $ID");
      unregister($socket, $ID);
   }
   
   return $rval;
   
}


# Attempt to associate a socket
# with an referencable identity.
#
# Also takes in a socket handling
# function if the socket needs one
# to be set or shifted. If left undefined,
# then the socket's handle will not be
# touched and it will be only updated if
# the socket is not bound to an ID. This
# function will bind the socket with both
# entities if they are both supplied at once.
#
# Returns: 
#         0: fault
#         1: bind created
#         2: already bound without fault
sub coalesce
{
   my $socket = shift;
   my $ID = shift;
   my $handle = shift;
   my $rval = 2;
   
   my $_ID_1 = $SocketsToIDs{$socket};
   my $_socket_1 = $IDsToSockets{$_ID_1};
   my $_socket_2 = $IDsToSockets{$ID};
   
   my $faultFlag_1 = (defined($_ID_1) && ($_ID_1 ne $ID));
   my $faultFlag_2 = ($_socket_1 && ($_socket_1 != $socket));
   
   
   if ($faultFlag_1 || ($faultFlag_1 = $faultFlag_2))
   {
      myLog('FAULT_1: ' . $_ID_1);
      if ($faultFlag_2)
      {
         myLog('FAULT_2: ' . $_ID_1);
         unregister($_socket_1, $_ID_1);
         $_socket_1 = undef;
      }
   }
   
   
   $faultFlag_2 = ($_socket_2 && $_socket_2 != $socket);
   if ($faultFlag_1 || $faultFlag_2)
   {
      if ($faultFlag_2)
      {
         myLog('FAULT_3: ' . $ID);
         unregister($_socket_2, $ID);
         $_socket_2 = undef;
      }
      unregister($socket, $_ID_1);
      $rval = 0;
   }
   elsif (!$_socket_2)
   {
      if (defined $ID)
      {
         if(exists $IDsToSockets{$ID})
         {
            myLog("$ID is re-recognized");
         }
         else
         {
            myLog("$ID is now recognized");
         }
         $IDsToSockets{$ID} = $socket;
      }
      $SocketsToIDs{$socket} = $ID;
      if ($handle)
      {
         $SocketsToHandles{$socket} = $handle;
      }
      $rval = 1;
   }
   
   return $rval;
   
}


# returns bool, true if no error occured
# the return is merely cosmetic in nature
sub unregister
{
   my $socket = shift;
   my $_ID = shift || $SocketsToIDs{$socket};
   my $ID = $_ID;
   if (!defined $ID)
   {
      $ID = '<unverified>';
   }
   my $dropNumForID;
   my $err;
   
   
   $dropNumForID = 1;
   if (exists $StatsByIDs{$ID})
   {
      $dropNumForID += $StatsByIDs{$ID};
      $StatsByIDs{$ID} = $dropNumForID;
   }
   else
   {
      $StatsByIDs{$ID} = $dropNumForID;
   }
   
   if (defined $_ID) {
      delete $SocketsToIDs{$socket};
      $IDsToSockets{$_ID} = undef;
   }
   delete $SocketsToHandles{$socket};
   eval
   {
      $inputSelector->remove($socket) || ($err = 1);
      $socket->close() || ($err = 2);
   };
   if ($@)
   {
      myLog($@);
      $err = 3;
   }
   
   myLog("$ID dropped for the $dropNumForID time" . ($err ? "; ERR=$err" : ''));
   
   return !$err;
}


# returns a new socket connection
# undefined indicates a error may
# be present on the server socket
sub register
{
   my $serverSocket = shift;
   my $handle = shift;
   my $ID = shift;
   my $newSocket;
   my $err;
   eval '$newSocket = $serverSocket->accept();';
   if ($@)
   {
      $err = 1;
      myLog($@);
   }
   elsif($newSocket)
   {
      $inputSelector->add($newSocket) || ($err = 3);
   }
   else
   {
      $err = 2;
   }
   
   if ($err)
   {
      $newSocket = undef;
      myLog("Failed to register a new socket; ERR=$err");
   }
   else
   {
      if (!coalesce($newSocket, $ID, $handle))
      {
         $newSocket = undef;
      }
   }
   
   return $newSocket;
}


# Reserved for handling LOOPBACK Procs
sub handleCritical
{
   my $socket = shift;
   if ($socket == $LOOPBACK_OUT)
   {
      die 'Loopback disconnected';
   }
   my ($cmd, $readErr) = readLine($socket);
   if ($readErr)
   {
      die 'Loopback disconnected';
   }
   my $ID = $CMDsToIDs{$cmd};
   my $serverSocket = $IDsToSockets{$ID};
   my %config = %{$CMDsToConfigs{$cmd}};
   my $handle = $SocketsToHandles{$serverSocket} || die 'No handle passed';
   my $err;
   
   
   myLog("Restarting LocalPort $ID...");
   
   if (defined($serverSocket))
   {
      unregister($serverSocket, $ID);
   }
   
   eval '$serverSocket = IO::Socket::INET->new(%config);';
   if ($@)
   {
      $err = 1;
      myLog($@);
   }
   elsif($serverSocket)
   {
      $inputSelector->add($serverSocket) || ($err = 3);
   }
   else
   {
      $err = 2;
   }
   
   if (!$err)
   {
      if (!coalesce($serverSocket, $ID, $handle))
      {
         $err = 4;
         (print $LOOPBACK_OUT $cmd) || die 'Failed to send on LOOPBACK';
      }
   }
   
   if ($err)
   {
      $serverSocket = undef;
      myLog("LocalPort $ID failed to restart; ERR=$err");
   }
   
   
   if ($cmd eq $RESTART_SUBSCRIBER_CMD)
   {
      $SUBSCRIBER_SERVER_SOCKET = $serverSocket;
   }
   elsif ($cmd eq $RESTART_SENSOR_CMD)
   {
      $SENSOR_SERVER_SOCKET = $serverSocket;
   }
   else
   {
      die 'Unknown critical message on LOOPBACK';
   }
   
   return !$err;
}

# Returns a tuple of two scalars
# 0: string -> line of text
# 1: bool   -> indicates socket closure
sub readLine
{
   my $socket = shift;
   my @charArray;
   my $noErr = 1;
   
   while($noErr = sysread($socket, $_, $noErr))
   {
      push(@charArray, $_);
      last if ($_ eq "\n");
   }
   
   return join('', @charArray), !$noErr;
}


# Standard print of messages wrapped
# in a timestamp
# A return of true means the pipe is still open.
sub myLog
{
   my ($sec, $min, $hour, $mday, $mon, $year) = localtime(time);
   my $timestamp = sprintf('%4d-%02d-%02d %02d:%02d:%02d', $year + 1900, $mon + 1, $mday, $hour, $min, $sec);

   return (print "<$timestamp>" . join('', @_) . "</$timestamp>\n");
}
