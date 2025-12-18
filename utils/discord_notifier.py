from discord_webhook import DiscordWebhook
from datetime import datetime
from pytz import timezone
import os

class DiscordNotifier:
    def __init__(self, config_file, venue_name) -> None:
        self.venue_name = venue_name
        
        # load config
        try:
            self.config = config_file['discord']
        except Exception as e:
            self.logger.error('config not loaded properly for Discord Notifier', e)
        
        # load webhook_mappings
        self.webhook_mappings = {}
        webhook_mappings = self.config['webhook_mappings']
        for metric, url in webhook_mappings.items():
            self.webhook_mappings[metric] = url
            
        # load mention discord ids
        self.mention_discord_ids = self.config['mention_discord_ids']
        
    def send_message(self, files: dict, metric: str, param_to_display: dict, mention: bool):
        if mention:
            allow_mentions = {'users': self.mention_discord_ids}
            mention_str = ",".join( f'<@{x}>' for x in self.mention_discord_ids)
        else:
            allow_mentions = {'users': []}
            mention_str = ''
        
        # update content
        content = f'{mention_str}\n{param_to_display["title"]}'
        
        # construct webhook
        webhook = DiscordWebhook(
            url=self.webhook_mappings[metric],
            content=content,
            username=self.venue_name,
            allowed_mentions=allow_mentions
        )
        
        # add images
        for file_name, file_path in files.items():
            with open(file_path, "rb") as f:
                webhook.add_file(file=f.read(), filename=file_name)
            
        resp = webhook.execute()
        
        return resp.status_code

    def send_daily_checks(self, indicator_name: str, body: str, files: dict, mention: bool):
        if mention:
            allow_mentions = {'users': self.mention_discord_ids}
            mention_str = ",".join( f'<@{x}>' for x in self.mention_discord_ids)
        else:
            allow_mentions = {'users': []}
            mention_str = ''
        
        # update content
        content = f'{mention_str}\n{body}'
        
        # construct webhook
        webhook = DiscordWebhook(
            url=self.webhook_mappings[indicator_name],
            content=content,
            username=indicator_name,
            allowed_mentions=allow_mentions
        )
        
        # add images
        for file_name, file_path in files.items():
            with open(file_path, "rb") as f:
                webhook.add_file(file=f.read(), filename=file_name)
                
        resp = webhook.execute()
        
        return resp.status_code
    
class EmergencyExitDiscordNotifier:
    def __init__(self) -> None:
        self.webhook_url = os.getenv("DISCORD_EMERGENCY_WEBHOOK_URL")
        if not self.webhook_url:
            raise ValueError("Missing env var DISCORD_EMERGENCY_WEBHOOK_URL")
    
    def notify(self):
        curr_ts = datetime.now(tz=timezone('Asia/Singapore'))
        
        webhook = DiscordWebhook(
            url=self.webhook_url,
            content=f"{datetime.now(tz=timezone('Asia/Singapore')).strftime('%Y/%m/%d %H:%M:%S')} - script is down",
            username="shutdown_notifier",
        )
        
        resp = webhook.execute()
        
        return resp.status_code
