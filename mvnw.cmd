@REM ----------------------------------------------------------------------------
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements. See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership. The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License. You may obtain a copy of the License at
@REM
@REM   https://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied. See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM ----------------------------------------------------------------------------

@REM Apache Maven Wrapper — downloads Maven 3.9.6 on first use, then delegates.

@ECHO OFF
SETLOCAL

SET SCRIPT_DIR=%~dp0
SET WRAPPER_PROPS=%SCRIPT_DIR%.mvn\wrapper\maven-wrapper.properties

IF NOT EXIST "%WRAPPER_PROPS%" (
    ECHO Error: %WRAPPER_PROPS% not found. >&2
    EXIT /B 1
)

@FOR /F "tokens=2 delims==" %%A IN ('findstr /B "distributionUrl" "%WRAPPER_PROPS%"') DO SET DISTRIBUTION_URL=%%A

@FOR %%F IN ("%DISTRIBUTION_URL%") DO SET ZIP_NAME=%%~nxF

@REM Strip -bin.zip suffix to get extracted directory name (apache-maven-X.Y.Z)
SET DIST_DIR_NAME=%ZIP_NAME:-bin.zip=%

IF DEFINED MAVEN_USER_HOME (
    SET DISTS_BASE=%MAVEN_USER_HOME%\wrapper\dists
) ELSE (
    SET DISTS_BASE=%USERPROFILE%\.m2\wrapper\dists
)

SET MAVEN_HOME=%DISTS_BASE%\%DIST_DIR_NAME%

IF NOT EXIST "%MAVEN_HOME%\bin\mvn.cmd" (
    IF NOT EXIST "%DISTS_BASE%" MKDIR "%DISTS_BASE%"
    ECHO Downloading %DISTRIBUTION_URL% ...
    SET TMP_ZIP=%DISTS_BASE%\%ZIP_NAME%
    powershell -Command "Invoke-WebRequest -Uri '%DISTRIBUTION_URL%' -OutFile '%TMP_ZIP%'"
    powershell -Command "Expand-Archive -Path '%TMP_ZIP%' -DestinationPath '%DISTS_BASE%' -Force"
    DEL "%TMP_ZIP%"
    ECHO Maven installed to %MAVEN_HOME%
)

IF "%JAVA_HOME%"=="" (
    WHERE java >NUL 2>&1
    IF ERRORLEVEL 1 (
        ECHO Error: JAVA_HOME is not set and java is not on the PATH. >&2
        EXIT /B 1
    )
)

"%MAVEN_HOME%\bin\mvn.cmd" %*
