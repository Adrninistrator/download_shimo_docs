import json
import logging
import os
import time
import urllib.parse
from datetime import datetime
from typing import Dict, List, Any, Optional

import requests


class DocumentSystemDownloader:
    def __init__(self, config_file: str = "config.properties"):
        self.config = self._read_config(config_file)
        self.sleep_time_seconds = float(self.config.get("sleep_time_seconds", 0.0))
        self.base_url = "https://shimo.im/"
        self.session = requests.Session()
        self.session.headers.update({
            "X-Requested-With": "SOS 2.0",
            "Cookie": self.config["cookie"]
        })

        # 设置日志
        self._setup_logging()

    def _read_config(self, config_file: str) -> Dict[str, str]:
        """读取properties格式的配置文件"""
        config = {}
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        config[key.strip()] = value.strip()
        except FileNotFoundError:
            raise Exception(f"配置文件 {config_file} 不存在")
        except Exception as e:
            raise Exception(f"读取配置文件失败: {e}")

        required_keys = ["sleep_time_seconds", "root_folder_guid", "local_root_dir", "cookie"]
        for key in required_keys:
            if key not in config:
                raise Exception(f"配置文件中缺少必需的参数: {key}")

        return config

    def _setup_logging(self):
        """设置日志系统"""
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_dir = "log"
        os.makedirs(log_dir, exist_ok=True)

        # 主日志文件
        main_log_file = os.path.join(log_dir, f"{current_time}.log")
        # 支持格式的日志文件
        download_file_log_file = os.path.join(log_dir, f"download_file_{current_time}.log")

        # 配置根日志
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),  # 控制台输出
                logging.FileHandler(main_log_file, encoding='utf-8')  # 主日志文件
            ]
        )

        # 创建特定格式的日志器
        self.download_file_logger = logging.getLogger("download_file")
        self.download_file_logger.setLevel(logging.INFO)
        self.download_file_logger.addHandler(logging.FileHandler(download_file_log_file, encoding='utf-8'))
        self.download_file_logger.propagate = False

        self.logger = logging.getLogger(__name__)

    def _safe_filename(self, filename: str) -> str:
        """对文件名进行安全编码，仅编码特殊字符"""
        # Windows不支持的字符
        invalid_chars = '<>:"/\\|?*'
        encoded_parts = []

        for char in filename:
            if char in invalid_chars:
                encoded_parts.append(urllib.parse.quote(char))
            else:
                encoded_parts.append(char)

        return ''.join(encoded_parts)

    def _make_request(self, url: str, is_json: bool = True) -> Optional[Any]:
        if self.sleep_time_seconds > 0:
            self.logger.info(f"请求之前等待指定时间，单位秒: {self.sleep_time_seconds}")
            time.sleep(self.sleep_time_seconds)

        """发送HTTP请求并记录日志"""
        full_url = self.base_url + url if not url.startswith('http') else url
        try:
            self.logger.info(f"请求URL: {full_url}")
            response = self.session.get(full_url)
            response.raise_for_status()

            if is_json:
                result = response.json()
                self.logger.info(f"请求成功，返回JSON数据")
                return result
            else:
                self.logger.info(f"请求成功，返回二进制数据")
                return response.content

        except requests.RequestException as e:
            self.logger.error(f"请求失败: {e}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON解析失败: {e}")
            return None

    def get_folder_contents(self, folder_guid: str) -> Optional[List[Dict]]:
        """获取指定目录下的内容"""
        url = f"lizard-api/files?folder={folder_guid}"
        return self._make_request(url)

    def download_regular_file(self, file_guid: str) -> Optional[bytes]:
        """下载普通文件"""
        url = f"lizard-api/files/{file_guid}/download"
        return self._make_request(url, is_json=False)

    def export_office_file(self, file_guid: str, file_type: str) -> Optional[bytes]:
        """导出在线文档文件（三步流程）"""
        # 第一步：获取任务ID
        type_mapping = {
            "presentation": "pptx",
            "newdoc": "docx",
            "modoc": "docx",
            "mosheet": "xlsx"
        }
        export_type = type_mapping.get(file_type)
        if not export_type:
            self.logger.error(f"不支持的文件类型: {file_type}")
            return None

        url = f"lizard-api/office-gw/files/export?type={export_type}&fileGuid={file_guid}"
        first_response = self._make_request(url)
        if not first_response or first_response.get("status") != 0:
            self.logger.error("第一步导出请求失败")
            return None

        task_id = first_response.get("taskId")
        if not task_id:
            self.logger.error("未获取到taskId")
            return None

        # 第二步：轮询导出进度
        max_retries = 30
        retry_interval = 2  # 2秒

        for _ in range(max_retries):
            progress_url = f"lizard-api/office-gw/files/export/progress?taskId={task_id}"
            progress_response = self._make_request(progress_url)

            if (progress_response and
                    progress_response.get("status") == 0 and
                    progress_response.get("code") == 0):

                data = progress_response.get("data", {})
                progress = data.get("progress", 0)

                if progress == 100:
                    download_url = data.get("downloadUrl")
                    if download_url:
                        # 第三步：下载文件
                        file_content = self._make_request(download_url, is_json=False)
                        return file_content
                    break

            time.sleep(retry_interval)

        self.logger.error("导出文件超时或失败")
        return None

    def download_file(self, item: Dict, current_path: str):
        """下载单个文件"""
        file_guid = item["guid"]
        file_name = item["name"]
        file_type = item["type"]

        save_dir = os.path.join(self.config["local_root_dir"], current_path)
        logger = self.download_file_logger

        os.makedirs(save_dir, exist_ok=True)

        # 处理文件名
        safe_filename = self._safe_filename(file_name)
        file_path = os.path.join(save_dir, safe_filename)

        # 下载文件
        if file_type in ["presentation", "newdoc", "modoc", "mosheet"]:
            file_content = self.export_office_file(file_guid, file_type)
            # 添加正确的文件扩展名
            ext_mapping = {
                "presentation": ".pptx",
                "newdoc": ".docx",
                "modoc": ".docx",
                "mosheet": ".xlsx"
            }
            if file_content and file_type in ext_mapping:
                file_path += ext_mapping[file_type]
        else:
            file_content = self.download_regular_file(file_guid)

        if file_content:
            try:
                with open(file_path, 'wb') as f:
                    f.write(file_content)
                logger.info(f"下载成功: {os.path.join(current_path, safe_filename)}")
                self.logger.info(f"文件保存成功: {file_path}")
            except Exception as e:
                logger.error(f"保存文件失败: {os.path.join(current_path, safe_filename)} - {e}")
                self.logger.error(f"保存文件失败: {file_path} - {e}")
        else:
            logger.error(f"下载失败: {os.path.join(current_path, safe_filename)}")
            self.logger.error(f"下载文件失败: {file_name}")

    def traverse_folder(self, folder_guid: str, current_path: str = ""):
        """递归遍历目录"""
        self.logger.info(f"开始遍历目录: {current_path or '根目录'}")

        contents = self.get_folder_contents(folder_guid)
        if not contents:
            self.logger.error(f"获取目录内容失败: {current_path}")
            return

        for item in contents:
            if item.get("isFolder"):
                # 处理子目录
                subfolder_name = self._safe_filename(item["name"])
                subfolder_path = os.path.join(current_path, subfolder_name)
                self.logger.info(f"进入子目录: {subfolder_path}")
                self.traverse_folder(item["guid"], subfolder_path)
            else:
                # 处理文件
                self.logger.info(f"处理文件: {os.path.join(current_path, item['name'])}")
                self.download_file(item, current_path)

    def run(self):
        """启动下载任务"""
        self.logger.info("开始下载任务")
        self.logger.info(f"每次请求之间的时间间隔: {self.config['sleep_time_seconds']}")
        self.logger.info(f"根目录GUID: {self.config['root_folder_guid']}")
        self.logger.info(f"本地保存路径: {self.config['local_root_dir']}")

        # 创建本地根目录
        os.makedirs(self.config["local_root_dir"], exist_ok=True)

        # 开始遍历
        self.traverse_folder(self.config["root_folder_guid"])

        self.logger.info("下载任务完成")


if __name__ == "__main__":
    try:
        downloader = DocumentSystemDownloader("config.properties")
        downloader.run()
    except Exception as e:
        print(f"程序执行失败: {e}")
