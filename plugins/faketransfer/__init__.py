import json
import os
import pickle
from datetime import datetime
from typing import Any, List, Dict, Tuple

import requests
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, Request

from app import schemas
from app.core.config import settings
from app.chain.transfer import TransferChain
from app.core.context import MediaInfo
from app.core.metainfo import MetaInfoPath
from app.core.security import verify_uri_apikey
from app.db.transferhistory_oper import TransferHistoryOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import MediaType


class FakeTransfer(_PluginBase):
    # 插件名称
    plugin_name = "虚拟转移"
    # 插件描述
    plugin_desc = "虚拟转移"
    # 插件图标
    plugin_icon = "faketransfer.png"
    # 插件版本
    plugin_version = "0.3"
    # 插件作者
    plugin_author = "xcehnz"
    # 作者主页
    author_url = "https://github.com/xcehnz"
    # 插件配置项ID前缀
    plugin_config_prefix = "faketransfer_"
    # 加载顺序
    plugin_order = 15
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    transfer_his = None
    _transfer_type = 'move'
    _transfer = None
    _aliyun_host = 'https://openapi.aliyundrive.com'

    # 页面配置属性
    _enabled = False
    _notify = False
    _alist_host = ''
    _alist_token = ''
    _alist_sync_folder = ''
    _sync_cron = ''
    #
    _alist_storage_id = 0
    _aliyun_drive_id = ''
    _aliyun_parent_file_id = ''
    _max_hour = ''
    _clean_rcon = ''

    def init_plugin(self, config: dict = None):
        self._transfer = TransferChain()
        self.transfer_his = TransferHistoryOper()
        if config:
            self._enabled = config.get("enabled")
            self._notify = config.get("notify")
            self._alist_host = config.get("alist_host")
            self._alist_token = config.get("alist_token")
            self._alist_sync_folder = config.get("alist_sync_folder")
            self._alist_storage_id = config.get("alist_storage_id")
            self._sync_cron = config.get("sync_cron")
            self._aliyun_drive_id = config.get("aliyun_drive_id")
            self._aliyun_parent_file_id = config.get("aliyun_parent_file_id")
            self._max_hour = config.get("max_hour")
            self._clean_rcon = config.get("clean_rcon")

            mtp = config.get("manual_transfer_path", None)
            if mtp:
                logger.warn('执行单次转移...')
                self.update_config({
                    "enabled": self._enabled,
                    "notify": self._notify,
                    "alist_host": self._alist_host,
                    "alist_token": self._alist_token,
                    "alist_sync_folder": self._alist_sync_folder,
                    "alist_storage_id": self._alist_storage_id,
                    "sync_cron": self._sync_cron,
                    "aliyun_drive_id": self._aliyun_drive_id,
                    "aliyun_parent_file_id": self._aliyun_parent_file_id,
                    "max_hour": self._max_hour,
                    "clean_rcon": self._clean_rcon,
                })
                self.fake_transfer(mtp)

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        ret = []
        if self._enabled:
            if self._sync_cron:
                ret.append({
                    "id": "FakeTransfer",
                    "name": "虚拟转移",
                    "trigger": CronTrigger.from_crontab(self._sync_cron),
                    "func": self.fake_transfer,
                    "kwargs": {}
                })

            if self._clean_rcon:
                ret.append({
                    "id": "CleanRapidUpload",
                    "name": "定时清理上传文件",
                    "trigger": CronTrigger.from_crontab(self._clean_rcon),
                    "func": self._aliyun_clean_upload,
                    "kwargs": {}
                })

        return ret

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return [{
            "path": "/rapid_upload",
            "endpoint": self.rapid_upload,
            "methods": ["POST"],
            "summary": "秒传",
            "description": "秒传",
        }]

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
                            },
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
                                            'model': 'notify',
                                            'label': '启用通知',
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 6
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "alist_host",
                                            "label": "Alist host"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 6
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "alist_token",
                                            "label": "Alist token"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {
                                    "cols": 12,
                                    "md": 6
                                },
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "alist_sync_folder",
                                            "label": "同步目录"
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'sync_cron',
                                            'label': '目录自动同步周期',
                                            'placeholder': '5位cron表达式，留空关闭'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'manual_transfer_path',
                                            'label': '手动同步目录',
                                            'placeholder': '单次转移目录，留空不转移'
                                        }
                                    }
                                ]
                            },
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
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'info',
                                            'variant': 'tonal',
                                            'text': '注意：以下配置属于高级设置，不清楚的请勿配置。'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'max_hour',
                                            'label': '超时时间',
                                            'placeholder': '加速文件最长保留时间，单位：小时'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'clean_rcon',
                                            'label': '加速文件清理周期',
                                            'placeholder': '5位cron表达式，留空默认6小时'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'alist_storage_id',
                                            'label': '用于加速的存储id',
                                            'placeholder': '纯数字'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'aliyun_drive_id',
                                            'label': '阿里云盘存储id',
                                            'placeholder': 'drive id'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'aliyun_parent_file_id',
                                            'label': '阿里云盘目录',
                                            'placeholder': '加速目录id'
                                        }
                                    }
                                ]
                            },
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": False,
            "cron": '',
            "alist_host": '',
            "alist_token": '',
            "alist_sync_folder": '',

            "alist_storage_id": 0,
            "aliyun_drive_id": '',
            "aliyun_parent_file_id": '',
            "clean_rcon": '',
            "max_hour": 0,
        }

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        """
        退出插件
        """
        pass

    async def rapid_upload(self, request: Request, _: str = Depends(verify_uri_apikey)):
        data = await request.json()

        file_name = data.get("file_name", None)
        size = int(data.get("size", 0))
        sha1 = data.get("sha1", None)
        abs_path = data.get("abs_path", None)

        if (not size or not sha1) and (not abs_path or not os.path.exists(abs_path)):
            return schemas.Response(success=False, message="参数错误")

        if abs_path:
            file_name = os.path.basename(abs_path)
            with open(abs_path, 'rb') as f:
                data_load = pickle.load(f)
                size = data_load.get('size', 0)
                sha1 = data_load.get('sha1', '')

        if not file_name:
            file_name = datetime.now().strftime('%Y%m%d%H%M%S%f') + '.mkv'

        dl_url = self._aliyun_download_url(file_name, size, sha1)
        return schemas.Response(success=True if dl_url else False, data={
            "url": dl_url
        })

    def fake_transfer(self, path=None):
        logger.info(f'开始执行目录{path if path else self._alist_sync_folder}转移任务...')

        if path:
            file_list = self._alist_list(path)
        else:
            file_list = self._alist_list(self._alist_sync_folder)
        if not file_list:
            return

        for file in file_list:
            self.__do_fake_transfer(file)

    def __do_fake_transfer(self, file):
        ths = self.transfer_his.get_by_src(file['path'])
        if ths:
            logger.debug(f'文件{file["path"]}已经转移过, 不再转移')
            return
        file_temp_dir = settings.TEMP_PATH.joinpath(os.path.dirname(file['path'])[1:])
        file_path = file_temp_dir / file['name']

        if not os.path.exists(file_temp_dir):
            os.makedirs(file_temp_dir)

        with open(file_path, 'wb') as f:
            data = {
                'src': file['path'],
                'size': file['size'],
                'sha1': file['sha1'],
            }
            pickle.dump(data, f)

        file_meta = MetaInfoPath(file_path)
        if not file_meta.name:
            logger.error(f"{file_path.name} 无法识别有效信息")
            return
        mediainfo = self.chain.recognize_media(meta=file_meta)
        fake_dir = settings.LIBRARY_PATHS[0] / self.__get_dest_dir(mediainfo) / 'fake'

        # 开始转移
        state, errmsg = self._transfer.do_transfer(
            path=file_path,
            mediainfo=mediainfo,
            target=fake_dir,
            transfer_type=self._transfer_type,
        )
        if not state:
            logger.error(f'转移文件：{file["path"]}，结果：{errmsg}')

    @staticmethod
    def __get_dest_dir(mediainfo: MediaInfo) -> str:
        if mediainfo.type == MediaType.MOVIE:
            return settings.LIBRARY_MOVIE_NAME

        if mediainfo.type == MediaType.TV:
            # 电视剧
            if mediainfo.genre_ids \
                    and set(mediainfo.genre_ids).intersection(set(settings.ANIME_GENREIDS)):
                # 动漫
                return settings.LIBRARY_ANIME_NAME or settings.LIBRARY_TV_NAME
            else:
                # 电视剧
                return settings.LIBRARY_TV_NAME
        return settings.LIBRARY_MOVIE_NAME

    def _alist_list(self, path, recursion=True, pwd=None):
        if not self._alist_host:
            return {}

        url = f'{self._alist_host}/api/fs/list'
        headers = {'Content-Type': 'application/json'}

        if self._alist_token:
            headers.update({'Authorization': f'{self._alist_token}'})

        if pwd:
            headers.update({'Cookie': f'browser-password={pwd}'})

        def list_dir(alist_path):
            data = {
                "path": alist_path,
                "password": '',
                "page": 1,
                "per_page": 0,
                "refresh": bool(self._alist_token)
            }
            response = requests.post(url, headers=headers, data=json.dumps(data))
            return response.json()

        file_list = []

        def list_all(root):

            data = list_dir(root)
            if not data['data']['content']:
                return
            for item in data['data']['content']:
                if item['is_dir']:
                    if recursion:
                        list_all(root + "/" + item['name'])
                else:
                    if os.path.splitext(item['name'])[-1].lower() not in settings.RMT_MEDIAEXT:
                        continue
                    hash_info = item.get('hash_info') or {}
                    file_info = {
                        'name': item['name'],
                        'size': item['size'],
                        'sha1': hash_info.get('sha1', None),
                        'path': f'{root}/{item["name"]}',
                    }
                    file_list.append(file_info)

        list_all(path)
        return file_list

    def _alist_storage(self, storage_id):
        if not self._alist_host:
            return {}

        url = f'{self._alist_host}/api/admin/storage/get?id={storage_id}'
        headers = {'Content-Type': 'application/json'}
        headers.update({'Authorization': f'{self._alist_token}'})
        response = requests.get(url, headers=headers)
        return response.json()

    def _get_access_token(self):
        storage_info = self._alist_storage(self._alist_storage_id)['data']
        if not storage_info:
            return None
        if 'Aliyun' not in storage_info['driver']:
            return None
        return json.loads(storage_info['addition'])['AccessToken']

    def _aliyun_upload(self, file_name, size, sha1):
        token = self._get_access_token()
        if not token:
            return None, {}
        url = f'{self._aliyun_host}/adrive/v1.0/openFile/create'
        payload = json.dumps({
            "drive_id": self._aliyun_drive_id,
            "parent_file_id": self._aliyun_parent_file_id,
            "type": "file",
            "name": file_name,
            "check_name_mode": "refuse",
            "size": size,
            "content_hash": sha1,
            "content_hash_name": "sha1",
            "proof_code": "DA76"
        })
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }

        response = requests.post(url, headers=headers, data=payload)

        if response.status_code == 200:
            return token, response.json()
        return None, {}

    def _aliyun_download_url(self, file_name, size, sha1):

        if not self._aliyun_drive_id or not self._aliyun_parent_file_id:
            return None

        token, upload_ret = self._aliyun_upload(file_name, size, sha1)
        if not upload_ret:
            return None
        file_id = upload_ret['file_id']
        url = f"{self._aliyun_host}/adrive/v1.0/openFile/getDownloadUrl"

        payload = json.dumps({
            "drive_id": self._aliyun_drive_id,
            "file_id": file_id
        })
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }

        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            return response.json()['url']
        return None

    def _aliyun_file_list(self):
        ret = []
        url = f"{self._aliyun_host}/adrive/v1.0/openFile/list"
        payload = json.dumps({
            "drive_id": self._aliyun_drive_id,
            "parent_file_id": self._aliyun_parent_file_id,
            "order_by": "created_at",
        })
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self._get_access_token()}'
        }
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code != 200:
            logger.error(f'获取文件列表失败: {response.text}')
            return ret
        for item in response.json()['items']:
            ret.append({
                'name': item['name'],
                'created_at': item['created_at'],
                'file_id': item['file_id'],
            })
        return ret

    def _aliyun_clean_upload(self):
        files = self._aliyun_file_list()
        for item in files:
            created_at = datetime.strptime(item['created_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            hours = int((datetime.now() - created_at).total_seconds() / 3600)
            logger.debug(f"文件{item['name']} 创建时间 {item['created_at']}")
            if hours >= 24:
                ret = self._delete_file(item['file_id'])
                logger.warn(f"文件{item['name']} 创建超过{hours}小时, 删除{'成功' if ret else '失败'}")

    def _delete_file(self, file_id):
        url = f'{self._aliyun_host}/adrive/v1.0/openFile/delete'
        payload = json.dumps({
            "drive_id": self._aliyun_drive_id,
            "file_id": file_id,
        })
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self._get_access_token()}'
        }
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            return True
        return False
