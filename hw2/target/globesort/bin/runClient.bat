@REM ----------------------------------------------------------------------------
@REM  Copyright 2001-2006 The Apache Software Foundation.
@REM
@REM  Licensed under the Apache License, Version 2.0 (the "License");
@REM  you may not use this file except in compliance with the License.
@REM  You may obtain a copy of the License at
@REM
@REM       http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.
@REM ----------------------------------------------------------------------------
@REM
@REM   Copyright (c) 2001-2006 The Apache Software Foundation.  All rights
@REM   reserved.

@echo off

set ERROR_CODE=0

:init
@REM Decide how to startup depending on the version of windows

@REM -- Win98ME
if NOT "%OS%"=="Windows_NT" goto Win9xArg

@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" @setlocal

@REM -- 4NT shell
if "%eval[2+2]" == "4" goto 4NTArgs

@REM -- Regular WinNT shell
set CMD_LINE_ARGS=%*
goto WinNTGetScriptDir

@REM The 4NT Shell from jp software
:4NTArgs
set CMD_LINE_ARGS=%$
goto WinNTGetScriptDir

:Win9xArg
@REM Slurp the command line arguments.  This loop allows for an unlimited number
@REM of arguments (up to the command line limit, anyway).
set CMD_LINE_ARGS=
:Win9xApp
if %1a==a goto Win9xGetScriptDir
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto Win9xApp

:Win9xGetScriptDir
set SAVEDIR=%CD%
%0\
cd %0\..\.. 
set BASEDIR=%CD%
cd %SAVEDIR%
set SAVE_DIR=
goto repoSetup

:WinNTGetScriptDir
set BASEDIR=%~dp0\..

:repoSetup
set REPO=


if "%JAVACMD%"=="" set JAVACMD=java

if "%REPO%"=="" set REPO=%BASEDIR%\repo

set CLASSPATH="%BASEDIR%"\conf;"%REPO%"\io\grpc\grpc-netty\1.6.1\grpc-netty-1.6.1.jar;"%REPO%"\io\grpc\grpc-core\1.6.1\grpc-core-1.6.1.jar;"%REPO%"\io\grpc\grpc-context\1.6.1\grpc-context-1.6.1.jar;"%REPO%"\com\google\errorprone\error_prone_annotations\2.0.19\error_prone_annotations-2.0.19.jar;"%REPO%"\com\google\code\findbugs\jsr305\3.0.0\jsr305-3.0.0.jar;"%REPO%"\com\google\instrumentation\instrumentation-api\0.4.3\instrumentation-api-0.4.3.jar;"%REPO%"\io\opencensus\opencensus-api\0.5.1\opencensus-api-0.5.1.jar;"%REPO%"\io\netty\netty-codec-http2\4.1.14.Final\netty-codec-http2-4.1.14.Final.jar;"%REPO%"\io\netty\netty-codec-http\4.1.14.Final\netty-codec-http-4.1.14.Final.jar;"%REPO%"\io\netty\netty-codec\4.1.14.Final\netty-codec-4.1.14.Final.jar;"%REPO%"\io\netty\netty-handler\4.1.14.Final\netty-handler-4.1.14.Final.jar;"%REPO%"\io\netty\netty-buffer\4.1.14.Final\netty-buffer-4.1.14.Final.jar;"%REPO%"\io\netty\netty-common\4.1.14.Final\netty-common-4.1.14.Final.jar;"%REPO%"\io\netty\netty-handler-proxy\4.1.14.Final\netty-handler-proxy-4.1.14.Final.jar;"%REPO%"\io\netty\netty-transport\4.1.14.Final\netty-transport-4.1.14.Final.jar;"%REPO%"\io\netty\netty-resolver\4.1.14.Final\netty-resolver-4.1.14.Final.jar;"%REPO%"\io\netty\netty-codec-socks\4.1.14.Final\netty-codec-socks-4.1.14.Final.jar;"%REPO%"\io\grpc\grpc-protobuf\1.6.1\grpc-protobuf-1.6.1.jar;"%REPO%"\com\google\protobuf\protobuf-java\3.3.1\protobuf-java-3.3.1.jar;"%REPO%"\com\google\guava\guava\19.0\guava-19.0.jar;"%REPO%"\com\google\protobuf\protobuf-java-util\3.3.1\protobuf-java-util-3.3.1.jar;"%REPO%"\com\google\code\gson\gson\2.7\gson-2.7.jar;"%REPO%"\com\google\api\grpc\proto-google-common-protos\0.1.9\proto-google-common-protos-0.1.9.jar;"%REPO%"\io\grpc\grpc-protobuf-lite\1.6.1\grpc-protobuf-lite-1.6.1.jar;"%REPO%"\io\grpc\grpc-stub\1.6.1\grpc-stub-1.6.1.jar;"%REPO%"\net\sourceforge\argparse4j\argparse4j\0.8.1\argparse4j-0.8.1.jar;"%REPO%"\ucsd\cse124\globesort-java\0.0.1-SNAPSHOT\globesort-java-0.0.1-SNAPSHOT.jar

set ENDORSED_DIR=
if NOT "%ENDORSED_DIR%" == "" set CLASSPATH="%BASEDIR%"\%ENDORSED_DIR%\*;%CLASSPATH%

if NOT "%CLASSPATH_PREFIX%" == "" set CLASSPATH=%CLASSPATH_PREFIX%;%CLASSPATH%

@REM Reaching here means variables are defined and arguments have been captured
:endInit

%JAVACMD% %JAVA_OPTS%  -classpath %CLASSPATH% -Dapp.name="runClient" -Dapp.repo="%REPO%" -Dapp.home="%BASEDIR%" -Dbasedir="%BASEDIR%" globesort.GlobeSortClient %CMD_LINE_ARGS%
if %ERRORLEVEL% NEQ 0 goto error
goto end

:error
if "%OS%"=="Windows_NT" @endlocal
set ERROR_CODE=%ERRORLEVEL%

:end
@REM set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" goto endNT

@REM For old DOS remove the set variables from ENV - we assume they were not set
@REM before we started - at least we don't leave any baggage around
set CMD_LINE_ARGS=
goto postExec

:endNT
@REM If error code is set to 1 then the endlocal was done already in :error.
if %ERROR_CODE% EQU 0 @endlocal


:postExec

if "%FORCE_EXIT_ON_ERROR%" == "on" (
  if %ERROR_CODE% NEQ 0 exit %ERROR_CODE%
)

exit /B %ERROR_CODE%
