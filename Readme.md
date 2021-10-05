# Slack Data Exporter
A python slack_data exporter
```
Export Slack history

optional arguments:
  -h, --help            show this help message and exit
  -t TOKEN, --token TOKEN
                        Slack API OAuth User Token
  -nz, --no_zip         Don't convert to zip
  -r                    export reminders to file
  -p                    Export public channels
  -d                    For download files to computer from chats

# Export data to zip without public/reminders/downloading files to computer
python slack_export.py --token xoxp-123...
or 
python slack_export.py -t xoxp-123...

# Export data to folder
python slack_export.py --token xoxp-123... -nz

# If you want download your future reminders use flag " -r "
# For download files to your computer use flag " -d "
```
If you later want to start the Slack Export archive viewer server, then need install slack-export-viewer, its not necessary
```
pip install slack-export-viewer
```
Then you can execute the viewer as documented
```
slack-export-viewer -z /path/to/your/ZipArchive.zip
```
