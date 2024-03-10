import os.path
import re
import time
from typing import Any, List, Dict, Tuple

from app.core.context import MediaInfo
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import TransferInfo
from app.schemas.types import EventType, MediaType


class AntiSpoil(_PluginBase):
    # 插件名称
    plugin_name = "隐藏剧透"
    # 插件描述
    plugin_desc = "移除媒体库的简介和标题剧透信息。"
    # 插件图标
    plugin_icon = "spoiler-alert.png"
    # 插件版本
    plugin_version = "0.2"
    # 插件作者
    plugin_author = "xcehnz"
    # 作者主页
    author_url = "https://github.com/xcehnz"
    # 插件配置项ID前缀
    plugin_config_prefix = "antispoil_"
    # 加载顺序
    plugin_order = 13
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    _enabled = False
    _delay = 0

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._delay = config.get("delay") or 0

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'delay',
                                            'label': '延迟时间（秒）',
                                            'placeholder': '0'
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "delay": 0
        }

    def get_page(self) -> List[dict]:
        pass

    @eventmanager.register(EventType.TransferComplete)
    def hide_plot(self, event: Event):
        """
        发送通知消息
        """
        if not self._enabled:
            return

        event_info: dict = event.event_data
        if not event_info:
            return

        mediainfo: MediaInfo = event_info.get("mediainfo")
        if mediainfo.type != MediaType.TV:
            return

        if self._delay:
            logger.info(f"延迟 {self._delay} 秒后刷新媒体库... ")
            time.sleep(float(self._delay))

        # 入库数据
        transferinfo: TransferInfo = event_info.get("transferinfo")
        for nf in transferinfo.file_list_new:
            nfo_path = os.path.splitext(nf)[0]
            nfo_file = nfo_path + '.nfo'
            tile = nfo_path.split('-')[-1].strip()
            if os.path.exists(nfo_file):
                try:
                    with open(nfo_file, 'r+', encoding='utf-8') as f:
                        logger.info(f'隐藏{nfo_file}剧情信息...')
                        nfo = f.read()
                        f.seek(0)
                        nfo = re.sub(r'<plot>.*?</plot>', '<plot />', nfo, flags=re.DOTALL)
                        nfo = re.sub(r'<outline>.*?</outline>', '<outline />', nfo, flags=re.DOTALL)
                        nfo = re.sub(r'<title>.*?</title>', f'<title>{tile}</title>', nfo, flags=re.DOTALL)
                        nfo = re.sub(r'<sorttitle>.*?</sorttitle>', f'<sorttitle>{tile}</sorttitle>', nfo,
                                     flags=re.DOTALL)
                        f.write(nfo)
                        f.truncate()
                except Exception as e:
                    logger.error('隐藏剧情信息失败：', e)

    def stop_service(self):
        """
        退出插件
        """
        pass
