echo off
REM 
REM access motherdck as second user
REM
echo this is john.apps@outlook.com, username john_apps_outlook
chcp 65001
REM
set motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzZXNzaW9uIjoiam9obi5hcHBzLm91dGxvb2suY29tIiwiZW1haWwiOiJqb2huLmFwcHNAb3V0bG9vay5jb20iLCJ1c2VySWQiOiJmZjQwM2Q0Yy1kNTVjLTQ1MTQtOGY1Zi0wMzQ4YmNiZTk1MmEiLCJpYXQiOjE3MTE4MjI1MzMsImV4cCI6MTc0MzM4MDEzM30.Hq8raVv3TPcMffYPOszDVWWq4IE0daHaQhaP6L8K-wE

rem
rem following token issused by john-d-apps
rem No good when accessing john.apps@outlook.com account
rem
rem set motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6ImpvaG4uZC5hcHBzQGdtYWlsLmNvbSIsInNlc3Npb24iOiJqb2huLmQuYXBwcy5nbWFpbC5jb20iLCJwYXQiOiI1N2FKazRWd2FlMGFDX2ViWWctbUVGSUZ2OEUydkRHbkRYZGRUZzQ4V1g0IiwidXNlcklkIjoiNzI2Mjc0MTEtZWYxOC00ZDRhLWE4NzctYjNlNWQ1MTk0YTlkIiwiaXNzIjoibWRfcGF0IiwiaWF0IjoxNzI4NjQ0NjcxfQ.0HR1iOvWGCnDlpFfUho_tETtJup1jBQ6a8KtPB8iZWs
title USER 4
prompt user-4$G