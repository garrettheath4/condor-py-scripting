"""
Garrett Heath Koller
Washington and Lee University
Python Condor
Python 3.1
This module makes it easy for users to submit command-line based programs
as jobs to a Condor system.  Essentially, the user can generate a list of
commands to submit on their own, instantiate a Condor object with the desired
settings, and then pass the list into a submit function.
"""

import sys
from io import BytesIO
import re
import subprocess
import shlex
import pickle
from platform import python_version_tuple, python_version
from time import sleep

if int(python_version_tuple()[0]) < 3:
    print("WARNING: You should use Python 3 to run this program,\nnot Python " + str(python_version()) + "!")

class Error(Exception):
    """Error() - Should not normally be directly called.
    Base class for exceptions in this module."""
    pass

class InvalidUniverseError(Error):
    """InvalidUniverseError(badUniverse)"""

    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value is None:
            return "Invalid Condor universe specified."
        return str(self.value) + " is not a valid Condor universe."

class TalkingToDeadError(Error):
    """TalkingToDeadError(deadProcessPid)"""

    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value is None:
            return "Unable to talk to a process because it is dead."
        return "Unable to talk to process " + str(self.value) \
               + " because it is dead."

class BadFormatError(Error):
    """BadFormatError(processName)"""

    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value: # is defined
            return "Unable to parse invalid output from process '" \
                   + str(self.value) + "'."
        return "Unable to parse process's invalid output."

class SubmissionError(Error):
    """SubmissionError(badMethodCall)"""

    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value: # is defined
            return "Cannot call '" + str(self.value) + "' because the job " \
                   + "has not been submitted yet."
        return "The job has not been submitted yet."

class SettingError(Error):
    """SettingError() - Should not normally be directly called.
    Base class for setting exceptions."""
    pass

class RequiredSetting(SettingError):
    """RequiredSetting(setting)"""

    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value is None:
            return "A setting is unexpectedly not set yet."
        return "The setting " + str(self.value) + " should have been set " \
               + "before doing this."

class EmptySetting(SettingError):
    """EmptySetting(setting)"""

    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value is None:
            return "The optional setting requested has not been set yet."
        return "The optional setting " + str(self.value) \
               + " has not been set yet."

class BadQuotes(SettingError):
    """BadQuotes(invalidCharacter)"""

    def __init__(self, value=None):
        self.value = value

    def __str__(self):
        if self.value is None:
            return "The supplied string contains improper quoting.  Escape " \
                    + "quotes properly and try again."
        if self.value == '"':
            return "The supplied string contains a double quote (\") that is " \
                    + "not escaped properly.  An entire argument with spaces " \
                    + "should be surrounded by single quotes instead ('). " \
                    + "Otherwise, try escaping the double quote with another" \
                    + "double quote (\"\")."
        return "The supplied string contains an invalid " + str(self.value) \
                + " character.  Remove this character and try again."

class Process(subprocess.Popen):
    """Process(args)
    This class masks the extra functionalities of 'subprocess.Popen' that
    just make it too confusing, including the fact that I/O is handled in
    bytes instead of as a string."""

    def __init__(self, args):
        self.args = args
        subprocess.Popen.__init__(self, args,
                                  stdout=subprocess.PIPE, stdin=subprocess.PIPE,
                                  stderr=subprocess.STDOUT,
                                  shell=True, executable="/bin/sh")
        self._savedOutput = ""

    def __str__(self):
        s = self.poll()
        if s is None:
            return "<Process (Running): " + str(self.args) + ">"
        return "<Process (Retval=" + str(s) + "): " + str(self.args) + ">"

    def get(self, ignoreEmpty=False):
        """get(ignoreEmpty=False) -> outputStr
        Fetch the standard output of the process and return it as a normal
        string.  Note that this method tells the process that it has reached
        the end of any input from stdin.  This method will wait for the process
        to end before returning the output.  If 'ignoreEmpty' is True, no error
        messages will be printed."""
        outBytes = bytes()
        try:
            # Flushes stdin, closes stdin, waits for the process to finish,
            # and reads stdout
            (outBytes, errBytes) = subprocess.Popen.communicate(self)
        except ValueError:
            if not ignoreEmpty:
                print("get: end of output.  Pid", self.pid, "is probably dead.",
                    file=sys.stderr)
                if not self.poll() is None:
                    print("get: Yup, pid", self.pid, "died a while ago.",
                        "Its final words were the retval",
                        str(self.poll()) + ".", file=sys.stderr)
        charString = ""
        for c in outBytes:
            charString += chr(c)
        retval = self._savedOutput + charString.strip()
        self._savedOutput = ""
        return retval

    def put(self, input):
        """put(string)
        Pass the string 'input' as the standard input to the process."""
        intInput = [ord(c) for c in input]
        byteInput = bytes(intInput)
        try:
            self.stdin.write(byteInput)
        except ValueError:
            raise TalkingToDeadError(self.pid)

    def getBytes(self, ignoreEmpty=False):
        """getBytes(ignoreEmpty=False) -> outputBytes
        Fetch the standard output of the process and return it as a bytes
        object.  Note that this method tells the process that it has reached
        the end of any input from stdin.  This method will wait for the process
        to end before returning the output.  If 'ignoreEmpty' is True, no error
        messages will be printed."""
        outBytes = bytes()
        try:
            # Flushes stdin, closes stdin, waits for the process to finish,
            # and reads stdout
            (outBytes, errBytes) = subprocess.Popen.communicate(self)
        except ValueError:
            if not ignoreEmpty:
                print("get: end of output.  Pid", self.pid, "is probably dead.",
                    file=sys.stderr)
                if not self.poll() is None:
                    print("get: Yup, pid", self.pid, "died a while ago.",
                        "Its final words were the retval",
                        str(self.poll()) + ".", file=sys.stderr)
        retval = bytes(self._savedOutput, 'ascii') + outBytes
        self._savedOutput = ""
        return retval

    def finish(self):
        """finish()
        Notify the process that you are done entering input into stdin.
        Wait for the proess to finish and then return.  Any resulting output
        will be saved to the object and can be read later with the get()
        function."""
        self._savedOutput += self.get(True)

    def terminate(self):
        """terminate()
        Nicely stop the running process.  Print an error message if it is not
        running."""
        try:
            subprocess.Popen.terminate(self)
        except OSError:
            print("terminate: pid", self.pid, "is already dead.", file=sys.stderr)

    def kill(self):
        """kill()
        Immediately kill the running process.  Print an error message if it is
        not running."""
        try:
            subprocess.Popen.kill(self)
        except OSError:
            print("kill: pid", self.pid, "is already dead.", file=sys.stderr)

class Shell(object):
    """Shell(remoteServer=None, remoteUser=None)
    This class abstracts away the difference between running a command locally
    or though an SSH session.  It creates a shell with which commands can be
    executed.  If 'remoteServer' is 'None' or 'localhost', the commands are
    executed on the local machine and 'remoteUser' does not need to be
    supplied.  Otherwise, the commands are executed on a remote machine
    "remoteServer" as the user "remoteUser" via an SSH session.  If no
    'remoteUser' is specified, the the local username is used."""

    def __init__(self, remoteServer=None, remoteUser=None):
        self.local = remoteServer is None or remoteServer.lower() == "localhost"
        if self.local:
            # Run commands directly
            self.remoteServer = None
            self.remoteUser   = None
        else:
            # Run commands in an SSH session
            self.remoteServer = remoteServer
            if remoteUser is None:
                self.remoteUser = Process("whoami").get().strip()
            else:
                self.remoteUser = remoteUser

    def __str__(self):
        if self.local:
            shellType = "local machine"
        else:
            shellType = self.remoteUser + "@" + self.remoteServer
        return "<Shell: " \
               + shellType \
               + ">"

    def _buildFullCommand(self, commandString):
        """_buildFullCommand(commandString) -> fullCommandString
        Given the 'commandString', a full command string is returned based
        on whether the shell is local or not.  If the shell is not local, the
        SSH command is prefixed to it and returned.  If the shell is local,
        the supplied 'commandString' is simlpy returned."""
        if self.local:
            return commandString
        else:
            retval = "ssh "
            if self.remoteUser:
                # remoteUser is defined and non-empty
                retval += str(self.remoteUser) + "@"
            return retval + str(self.remoteServer) + " " + commandString

    def execute(self, commandString, inputStr=None, returnBytes=False):
        """execute(commandString, inputStr=None, returnBytes=False) -> (returnValue, outputStr)
        Execute the given 'commandString' in a non-interactive shell.  The
        shell will wait for the process to finish before printing its output,
        if any.  If 'input' is supplied as a string, it is supplied to the
        process at runtime."""
        p = Process(self._buildFullCommand(commandString))
        if not inputStr is None:
            p.put(inputStr)
        if returnBytes:
            s = p.getBytes()
        else:
            s = p.get()
        return (p.poll(), s)

    def executeInteractive(self, commandString):
        """executeInteractive(commandString)
        Execute the given 'commandString' in an interactive shell.  If the
        process needs input the user will be prompted to supply it.  If the
        output is delayed, the user will be prompted to manually poll for it.
        The user is also given the option of killing the process."""
        p = Process(self._buildFullCommand(commandString))
        if not p.poll() is None:
            # The program finished quickly, so just print the output.
            print(p.get())
        while p.poll() is None:
            # The program is still running.  It is probably waiting for input.
            print("Process running.  What do you want to do?")
            needIn = input("input, output, terminate, kill, help: ")
            if needIn.lower() in ["input", "i", "in", "inp"]:
                p.put(input("stdin: "))
            elif needIn.lower() in ["output", "o", "out", "print", "p"]:
                print(p.get())
            elif needIn.lower() in ["terminate", "term", "t", "te", "ter"]:
                p.terminate()
            elif needIn.lower() in ["kill", "k", "ki", "kil"]:
                p.kill()
            else:  # print help text
                print( \
"""The process has not finished yet.  It is either taking a while or it
is wating for your input.  To interact with it, choose from one of the
following options:
  input: Give the program some keyboard input.
  output: Check to see if the program has said anything else since just now
          Note that if you do this after the program is done reading input,
          it will print the output AND EXIT.
  terminate: Finish whatever the process was doing and nicely end the job.
  kill: Immediately end the process.  Do this if the process is being bad.
  help: This text.""" \
                )

class Job(object):
    """Job(universe='vanilla', username=None, server='condor.cs.wlu.edu')
    Instantiates a Condor object that acts as an interface to the given Condor
    server.  This object can be used to submit jobs using a familiar Python
    environment."""

    def __init__(self, universe="vanilla",
                 username=None, server="condor.cs.wlu.edu"):
        self._validUniverses = ["vanilla", "standard", "java",
                                "scheduler", "local", "grid", "vm"]
        self._settings = {}
        self.submitLinesSoFar = ""
        self._submitShell = None
        self._executablePath = ""
        self.cluster = None
        self.maxPollTime = 30.0
        try:
            self._submitterUsername = Process('whoami').get().strip()
        except:
            self._submitterUsername = None
        self.setUniverse(universe)
        self._setUsername(username)
        self._setServer(server)
        self.setCPUNum(1)
        self.setRAM(1024)
        self.setDiskSpace(32)

        p = Process("which condor_submit")
        if p.wait() == 0 and self.getUsername() == self._submitterUsername:
            # condor_submit exists, so run it locally
            # UNLESS the usernames don't match
            self._submitShell = Shell()
        else:
            # condor_submit doesn't exist, so run it over SSH
            self._submitShell = Shell(self.getServer(), self.getUsername())
            # Set the default directory to the current directory
            # of the local user

        self.setEmail(None)

    def __str__(self):
        return "<Job: " \
               + str(self.getUsername()) + "@" + str(self.getServer()) + "\n" \
               + self._generateSubmitString(False).strip() \
               + ">"

    def getUniverse(self):
        """getUniverse() -> string
        Returns the currently chosen Condor universe that this object
        will use when submitting jobs."""
        try:
            return self._settings["Universe"]
        except KeyError:
            raise RequiredSetting("Universe")

    def setUniverse(self, string):
        """setUniverse(string)
        Sets the Condor universe that this object will use when
        submitting jobs."""
        if not string in self._validUniverses:
            raise InvalidUniverseError(string)
        self._settings["Universe"] = string

    def getUsername(self):
        """getUsername() -> string
        Returns the username that this object will use to submit the job
        to Condor via the designated Condor submit server.  Note that this
        object will access the submit server with this username through SSH."""
        return self.username

    def _setUsername(self, string):
        """_setUsername(string)
        Sets the username that this object will use to submit the job to
        Condor via the designated Condor submit server.  Note that this
        object will access the submit server with this username through SSH.
        If 'string' is 'None', the username will be set to the output of the
        'whoami' command."""
        if string is None:
            self.username = self._submitterUsername
        else:
            self.username = string

    def getServer(self):
        """getServer() -> string
        Returns the hostname of the server that this object will submit
        jobs to.  By default, this is the central manager of the Condor cluster.
        Note that this server should be allow remote logins through SSH."""
        return self.server

    def _setServer(self, string):
        """_setServer(string)
        Sets the hostname of the server that this object will submit jobs to.
        This is usually the central manager of the Condor cluster, but it can
        also just be a submit machine of the Condor cluster.  Note that this
        server should allow remote logins through SSH."""
        self.server = string

    def getExecutable(self):
        try:
            return self._settings["Executable"]
        except KeyError:
            raise RequiredSetting("Executable")

    def setExecutable(self, string):
        """setExecutable(string)
        Designates the binary file to be executed for the job.  Given a
        string, this method makes sure the "Executable" setting is fully
        qualified.  If it is a program, like matlab, its full pathname is
        required.  If it is a user-created program in the current directory,
        leave the value alone.  Don't use more than one executable per
        submission."""
        newPath = self._resolveExecutable(string)
        if "/" in newPath.replace("\\/", ""):
            self.setTransferExecutable(False)
        if not self._executablePath:
            # Haven't set or written the executable variable yet
            self._settings["Executable"] = newPath
            self._executablePath = self.getExecutable()
        else:
            # Executable was already set before
            # Warn if the user tries to make it something different
            if "Executable" in self._settings \
                    and self.getExecutable() != newPath:
                print("Warning: Generally speaking, only one executable should " \
                      "be used per submission.", file=sys.stderr)
                self._settings["Executable"] = newPath

    def getTransferExecutable(self):
        """getTransferExecutable() -> string"""
        try:
            return self._settings["Input"]
        except KeyError:
            raise EmptySetting("TransferExecutable")

    def setTransferExecutable(self, boolean):
        """setTransferExecutable(boolean)"""
        if type(boolean) != bool:
            raise TypeError("setTransferExecutable(): bool argument " \
                            + "expected, but " + str(type(boolean)) \
                            + " given.")
        self._settings["transfer_executable"] = bool(boolean)

    def _resolveExecutable(self, string):
        """_resolveExecutable(string) -> string
        Given a string of an executable's filename, returns the
        cannonicalized location of the executable if it is an
        application that is not in the current directory.  If it is an
        executable that is in the current directory, it returns the
        given string.  This is a function that does not modify the
        state of the object."""
        if self._submitShell.execute("ls " + str(string))[0] != 0:
            # The executable is not fully qualified or is not in the
            # current working directory
            (status, reply) = self._submitShell.execute("which " + str(string))
            if status == 0:
                # The executable was a standard application, so return
                # the full path to its executable
                return reply.strip()
            else:
                print("Warning: Could not find Executable:",
                      self.getExecutable(), file=sys.stderr)
                return string
        else:
            # The executable is either fully qualified or it is in the current
            # working directory
            return string

    def getArguments(self):
        """getArguments() -> string
        Returns the current set of arguments that condor will use for the
        next queue() or final submit()."""
        try:
            return self._settings["Arguments"]
        except KeyError:
            raise RequiredSetting("Arguments")

    def setArguments(self, string):
        """setArguments(string)"""
        self._settings["Arguments"] = "\"" + string + "\""

    def getInitialDirectory(self):
        """getInitialDirectory() -> string"""
        try:
            return self._settings["initialdir"]
        except KeyError:
            raise EmptySetting("InitialDirectory")

    def setInitialDirectory(self, string):
        """setInitialDirectory(string)"""
        self._settings["initialdir"] = string

    def getInput(self):
        """getInput() -> string"""
        try:
            return self._settings["Input"]
        except KeyError:
            raise EmptySetting("Input")

    def setInput(self, string):
        """setInput(string)"""
        self._settings["Input"] = string

    def getOutput(self):
        """getOutput() -> string"""
        try:
            return self._settings["Output"]
        except KeyError:
            raise EmptySetting("Output")

    def setOutput(self, string):
        """setOutput(string)"""
        self._settings["Output"] = string

    def getError(self):
        """getError() -> string"""
        try:
            return self._settings["Error"]
        except KeyError:
            raise EmptySetting("Error")

    def setError(self, string):
        """setError(string)"""
        self._settings["Error"] = string

    def getLog(self):
        """getLog() -> string"""
        try:
            return self._settings["Log"]
        except KeyError:
            raise EmptySetting("Log")

    def setLog(self, string):
        """setLog(string)"""
        self._settings["Log"] = string

    def getRequirements(self):
        """getRequirements() -> string"""
        try:
            return self._settings["Requirements"]
        except KeyError:
            raise EmptySetting("Requirements")

    def setRequirements(self, string):
        """setRequirements(string)"""
        self._settings["Requirements"] = string

    def getMatlabLock(self):
        """getMatlabLock() -> boolean"""
        try:
            return "matlab" in self._settings["concurrency_limits"].lower()
        except KeyError:
            raise EmptySetting("MatlabLock")

    def setMatlabLock(self, boolean):
        """setMatlabLock(boolean)"""
        if boolean:
            self._settings["concurrency_limits"] = "MATLAB"
        else:
            if "concurrency_limits" in self._settings:
                self._settings.pop("concurrency_limits")

    def getCPUNum(self):
        """getCPUNum() -> string"""
        try:
            return self._settings["request_cpus"]
        except KeyError:
            raise EmptySetting("request_cpus")

    def setCPUNum(self, integer):
        """setCPUNum(integer)"""
        self._settings["request_cpus"] = int(integer)

    def getRAM(self):
        """getRam() -> megabytes_int"""
        try:
            return self._settings["request_memory"]
        except KeyError:
            raise EmptySetting("request_RAM")

    def setRAM(self, megabytes_int):
        """setRAM(megabytes_int)"""
        self._settings["request_memory"] = int(megabytes_int)

    def getDiskSpace(self):
        """getDiskSpace() -> megabytes_int"""
        try:
            return self._settings["request_disk"]
        except KeyError:
            raise EmptySetting("request_diskSpace")

    def setDiskSpace(self, megabytes_int):
        """setDiskSpace(megabytes_int)"""
        self._settings["request_disk"] = int(megabytes_int)


    def getTransferFiles(self):
        """getTransferFiles() -> string"""
        try:
            return self._settings["should_transfer_files"]
        except KeyError:
            raise EmptySetting("should_transfer_files")

    def setTransferFiles(self, string):
        """setTransferFiles(string)"""
        self._settings["should_transfer_files"] = string

    def getWhenTransferOutput(self):
        """getWhenTransferOutput() -> string"""
        try:
            return self._settings["when_to_transfer_ouput"]
        except KeyError:
            raise EmptySetting("when_to_transfer_output")

    def setWhenTransferOutput(self, string):
        """setWhenTransferOutput(string)"""
        self._settings["when_to_transfer_output"] = string

    def getNotification(self):
        """getNotification() -> string"""
        try:
            return self._settings["notification"]
        except KeyError:
            raise EmptySetting("notification")

    def setNotification(self, string):
        """setNotification(string)"""
        self._settings["notification"] = string

    def getEmail(self):
        """getEmail() -> string"""
        try:
            return self._settings["notify_user"]
        except KeyError:
            raise EmptySetting("Email")

    def setEmail(self, string=None):
        """setEmail(string=None)
        Sets the email address of the submitter in case Condor wants to notify
        the user, such as if the job runs into an error.  If no argument is
        supplied, a predefined list of email mappings will be used to
        automatically map the user's username to their preferred email
        address."""
        if string:
            self._settings["notify_user"] = string
        else:
            # String empty or is None, so automatically detect email
            (status, bytesOut) = self._submitShell.execute( \
                    "cat /mnt/config/scripts/mail_map.pickle", returnBytes=True)
            if status == 0:
                emails = pickle.load(BytesIO(bytesOut))
                try:
                    self._settings["notify_user"] = emails[self.getUsername()]
                except KeyError:
                    print("Note:", self.getUsername(), "not in email mapping. ",
                          "Using a default value.", file=sys.stderr)
                    try:
                        self._settings["notify_user"] = emails[None]
                    except KeyError:
                        self._settings["notify_user"] = "kollerg14@mail.wlu.edu"
            

    def _generateSubmitString(self, update=True):
        """_generateSubmitString(update=True) -> submitString"""
        allLines = self.submitLinesSoFar
        for s in self._settings:
            allLines += str(s) + " = " + str(self._settings[s]) + "\n"
        if update:
            self._settings.clear()
            self.submitLinesSoFar = allLines
        return allLines

    def saveSubmitFile(self, filename):
        """saveSubmitFile(filename)
        Generate the submit file and save it as the given filename.
        To save the submit file so you can manually submit your job,
        this method should be run in lieu of calling the submit()
        method after setting all of the desired variables and calling
        queue() for all of the desired commands, although this method
        can still be called after the job is submitted."""
        f = open(filename, 'w')
        f.write(self._generateSubmitString(False))
        f.close()

    def queue(self, command_line, times=1):
        """queue(command_line, times=1)
        Save all of the variables set so far into the job's submission
        data and enqueue the program and arguments from 'command_line'.
        The optional argument 'times' tells Condor how many times to run
        the same command.  Note that you may NOT use double quotes (")
        in your command line unless you escape them with another double
        quote ("").  Instead, use single quotes (').  If you need nested
        single quotes, escape a nested single quote with another single
        quote ('')."""
        #TODO
        # Remove leading or trailing spaces
        command_line = command_line.strip()
        if '"' in command_line.replace('""', ''):
            raise BadQuotes('"')
        self.setExecutable(shlex.split(command_line)[0])
        args = command_line.split(" ")
        executable = args.pop(0)
        while executable.endswith("\\"):
            executable += args.pop(0)
        # The rest of args are the actual arguments to the executable
        argStr = " ".join(args)
        if argStr:
            self.setArguments(argStr)
        # Set arguments in _settings
        #allArgs = " ".join(args[1:])
        #allArgs = ""
        #for arg in args[1:]:
        #    allArgs += " '" + arg.replace("'", "''").replace('"', '""') + "' "
        #if allArgs:
        #    self.setArguments(allArgs)
        self._generateSubmitString()
        if times != 1:
            self.submitLinesSoFar += "Queue " + str(times) + "\n"
        else:
            self.submitLinesSoFar += "Queue\n"

    def submit(self):
        """submit() -> cluster_int
        Uses all currently set variables to submit a job to the designated
        specified Condor submit server.  Returns the cluster id if the job
        is submitted successfully or None if the job encounters an error in
        submission."""
        retval, msg = self._submitShell.execute( \
            "condor_submit -remote " + self.server, self._generateSubmitString())
        if retval != 0:
            print("ERROR #" + str(retval) + ":", file=sys.stderr)
            print("WARNING: Since 'condor_submit' returned an error, your " \
                  + "job was probably not submitted.  If your job submitted " \
                  + "after all, this object will still not be able to " \
                  + "monitor its status.", file=sys.stderr)
            return None
        print(msg)
        clusterRE = re.search("(cluster )(\d+)", msg)
        if clusterRE is None: raise BadFormatError("condor_submit")
        clusterStr = clusterRE.group(2)
        if not clusterStr.isdigit(): raise BadFormatError("condor_submit")
        self.cluster = int(clusterStr)
        return self.cluster

    def _checkQueue(self):
        """_checkQueue() -> string"""
        if self.cluster:
            return self._submitShell.execute( \
                    'condor_q ' + str(self.cluster) \
                    + ' -format "%d." ClusterId -format "%d\n" ProcId')
        else:
            raise SubmissionError("_checkQueue()")

    def wait(self):
        """wait()
        Waits for all processes of the submitted job to finish before
        returning.  If the job hasn't been submitted yet, raises a
        SubmissionError."""
        currPollTime = 1.0
        if self.cluster is None:
            raise SubmissionError("wait()")
        (retval, msg) = self._checkQueue()
        if msg.strip(): print("Waiting for cluster " + str(self.cluster) \
                              + " to finish", end='')
        while str(self.cluster) in msg.strip():
            if retval != 0:
                print("ERROR #" + str(retval) + ":", str(mesg), file=sys.stderr)
            print('.', end='')
            sleep(min(currPollTime, self.maxPollTime))
            currPollTime += 0.5
            (retval, msg) = self._checkQueue()
        print()

    def poll(self):
        """poll() -> runningProcesses_int
        Returns the number of processes still running in this job.  Unlike
        wait(), poll() is non-blocking and so returns immediately.  If the job
        hasn't been submitted yet, raises a SubmissionError."""
        if self.cluster is None:
            raise SubmissionError("poll()")
        (retval, msg) = self._checkQueue()
        if retval != 0:
            print("ERROR #" + str(retval) + ":", str(mesg), file=sys.stderr)
            raise BadFormatError("condor_q")
        else:
            return msg.count(str(self.cluster) + '.')

    def status(self, outputFn=print):
        """status(outputFn=print) -> [retval of outputFn()]
        Prints the status of the job using outputFn() from a call to the
        'condor_q' program.  The optional argument specifies the form of
        output.  By default, the status information is simply printed to the
        screen (using Python's builtin print() method).  If the user calls
        status(str), for example, the information will be returned as a normal
        string.  If the job has not been submitted yet, a SubmissionError is
        raised."""
        if self.cluster is None:
            raise SubmissionError("status()")
        (retval, msg) = self._submitShell.execute( \
                'condor_q ' + str(self.cluster))
        if retval != 0:
            print("ERROR #" + str(retval) + ":", str(mesg), file=sys.stderr)
            raise BadFormatError("condor_q")
        else:
            return outputFn(msg)



def test():
    def debug(lst):
        for code in lst:
            print(">", code)
            exec(code)

    print("Testing Process()...")
    procCodes = ["pout = Process('whoami')", "print(pout)",
                     "print(pout.get())",
                 "pin = Process('cat')", "print(pin)",
                     "pin.put('Hellow Orld!')", "print(pin.get())"]
    debug(procCodes)

    print("\nTesting Shell()...")
    shellCodes = ["slocal = Shell()", "print(slocal)",
                      "print(slocal.execute('whoami'))",
                      "print(slocal.execute('cat', 'Hellow Orld from your machine!'))",
                  "sremote = Shell('condor.cs.wlu.edu', 'kollerg')",
                      "print(sremote)",
                      "print(sremote.execute('whoami'))",
                      "print(sremote.execute('cat', 'Hellow Orld from afar!'))"]
    debug(shellCodes)

    print("\nTesting Job()...")
    cndrCodes = ["global c; c = Job('vanilla', 'kollerg')", "print(c)",
                 "c.setOutput('whoami.out')", "c.setError('whoami.err')",
                 "c.setLog('whoami.log')",
                 "c.setInitialDirectory('/mnt/data/kollerTest/whoami/')",
                 "c.queue('whoami')", "print(c)", "c.submit(), c.wait()"]
    debug(cndrCodes)


if __name__ == "__main__":
    #test()
    pass
