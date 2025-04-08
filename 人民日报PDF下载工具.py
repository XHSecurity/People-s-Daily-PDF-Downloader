"""
人民日报PDF下载工具（PyMuPDF优化版）
改进点：
1. 使用PyMuPDF替代PyPDF2解决合并警告
2. 保持完整代理功能
3. 增强PDF合并稳定性
"""
import os
import re
import sys
import time
import shutil
import argparse
import logging
import datetime
from typing import Tuple, Optional, List
from urllib.parse import urlparse

import requests
import fitz  # PyMuPDF
from rich.progress import (
    Progress,
    BarColumn,
    DownloadColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
    TaskID,
)
from rich.console import Console
from rich.logging import RichHandler

# 基础目录配置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)

# 全局配置
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
TIMEOUT = 15
MAX_RETRIES = 3

# 初始化控制台和日志
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RichHandler(console=console, rich_tracebacks=True, show_path=False),
        logging.FileHandler(
            os.path.join(BASE_DIR, "logs/rmrb.log"),
            encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger("RMZ_Downloader")


def init_environment() -> Tuple[str, str]:
    """初始化环境并清理临时目录"""
    download_dir = os.path.join(BASE_DIR, "download")
    temp_dir = os.path.join(BASE_DIR, "temp")

    for dir_path in [download_dir, temp_dir]:
        os.makedirs(dir_path, exist_ok=True)

    # 清理临时目录
    for f in os.listdir(temp_dir):
        file_path = os.path.join(temp_dir, f)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            logger.warning(f"清理临时文件失败: {file_path} - {str(e)}")

    return temp_dir, download_dir


def validate_date(input_date: Optional[str]) -> str:
    """验证单个日期"""
    today = datetime.date.today()

    if not input_date:
        return today.strftime("%Y-%m/%d")

    formats = [
        "%Y%m%d",  # YYYYMMDD
        "%Y-%m-%d",  # ISO格式
        "%Y/%m/%d",  # 斜杠格式
        "%Y-%m/%d",  # 目标格式
        "%Y%m/%d",  # 混合格式
    ]

    for fmt in formats:
        try:
            dt = datetime.datetime.strptime(input_date, fmt).date()
            if dt > today:
                raise ValueError("日期不能超过今天")
            if dt.year < 2003:
                raise ValueError("仅支持2003年及之后的报纸")
            return dt.strftime("%Y-%m/%d")
        except ValueError:
            continue

    logger.error(f"无效日期格式: {input_date} (支持格式: YYYYMMDD/YYYY-MM-DD/YYYY-MM/dd等)")
    sys.exit(1)


def validate_date_range(range_str: str) -> List[datetime.date]:
    """验证并解析日期范围"""
    try:
        clean_str = re.sub(r"[^0-9]", "", range_str)
        if len(clean_str) != 16:
            raise ValueError("无效的日期范围格式")

        start_str = clean_str[:8]
        end_str = clean_str[8:]
        start_date = datetime.datetime.strptime(start_str, "%Y%m%d").date()
        end_date = datetime.datetime.strptime(end_str, "%Y%m%d").date()

        if start_date > end_date:
            raise ValueError("开始日期不能晚于结束日期")
        if end_date > datetime.date.today():
            raise ValueError("结束日期不能超过今天")
        if start_date.year < 2003:
            raise ValueError("仅支持2003年及之后的报纸")

        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_list.append(current_date)
            current_date += datetime.timedelta(days=1)

        return date_list
    except Exception as e:
        logger.error(f"日期范围解析失败: {str(e)}")
        sys.exit(1)


def parse_date(target_date: str) -> Tuple[str, str, str]:
    """解析日期为不同格式"""
    dt = datetime.datetime.strptime(target_date, "%Y-%m/%d")
    return (
        target_date,
        dt.strftime("%Y%m%d"),
        dt.strftime("%Y%m/%d")
    )


def safe_request(url: str, proxies: Optional[dict]) -> Optional[requests.Response]:
    """带重试机制的请求函数"""
    for _ in range(MAX_RETRIES):
        try:
            response = requests.get(
                url,
                headers=HEADERS,
                timeout=TIMEOUT,
                verify=False,
                allow_redirects=False,
                proxies=proxies
            )
            if response.status_code == 200:
                return response
            if response.status_code == 404:
                return None
        except Exception as e:
            logger.warning(f"请求失败: {url} - {str(e)}")
            time.sleep(1)
    return None


def get_page_info(old_url: str, new_url: str, proxies: Optional[dict]) -> Tuple[int, bool]:
    """获取页数并判断版本"""
    if response := safe_request(new_url, proxies):
        if (pages := len(re.findall(r'pageLink', response.text))) > 0:
            return pages, True

    if response := safe_request(old_url, proxies):
        return len(re.findall(r'nbs', response.text)), False

    logger.error("无法获取报纸信息，请检查网络或日期")
    sys.exit(1)


def download_pdf(url: str, filename: str, temp_dir: str,
                 progress: Progress, task_id: TaskID,
                 proxies: Optional[dict]) -> bool:
    """带进度条和重试的下载函数"""
    for retry in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, stream=True,
                                    timeout=30, proxies=proxies)
            response.raise_for_status()

            if 'application/pdf' not in response.headers.get('Content-Type', ''):
                logger.warning(f"非PDF内容: {url}")
                return False

            total_size = int(response.headers.get('content-length', 0))
            file_path = os.path.join(temp_dir, filename)

            progress.update(task_id, total=total_size, visible=True)

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=2 * 1024 * 1024):  # 增大块大小
                    if chunk:
                        f.write(chunk)
                        progress.update(task_id, advance=len(chunk))

            if os.path.getsize(file_path) >= 1024:
                return True
        except Exception as e:
            logger.warning(f"下载失败 ({retry + 1}/{MAX_RETRIES}): {filename} - {str(e)}")
            time.sleep(1)
    return False


def download_edition(target_date: str, temp_dir: str, output_dir: str,
                     progress: Optional[Progress] = None,
                     main_task: Optional[TaskID] = None,
                     proxies: Optional[dict] = None) -> bool | None:
    """主下载流程（支持外部进度条）"""
    old_fmt, file_fmt, new_fmt = parse_date(target_date)
    output_path = os.path.join(output_dir, f"People's.Daily.{file_fmt}.pdf")

    if os.path.exists(output_path):
        logger.info(f"文件已存在: {os.path.basename(output_path)}")
        return True

    old_cover = f"http://paper.people.com.cn/rmrb/html/{old_fmt}/nbs.D110000renmrb_01.htm"
    new_cover = f"http://paper.people.com.cn/rmrb/pc/layout/{new_fmt}/node_01.html"

    total_pages, is_new = get_page_info(old_cover, new_cover, proxies)
    logger.info(f"检测到{target_date}共{total_pages}页 ({'新版' if is_new else '旧版'})")

    local_progress = progress or Progress(
        TextColumn("[bold blue]{task.fields[filename]}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
        console=console,
    )
    use_local_progress = not progress

    success = 0
    task_id = TaskID(-1)

    try:
        if use_local_progress:
            local_progress.start()

        task_id = local_progress.add_task(
            "downloading",
            filename=f"{target_date} 初始化...",
            total=0,
            visible=True
        )

        if main_task and progress:
            progress.update(
                main_task,
                description=f"[cyan]下载 {target_date}",
                refresh=True
            )

        for page in range(1, total_pages + 1):
            if main_task and progress and progress.tasks[main_task].finished:
                raise KeyboardInterrupt()

            filename = f"rmrb{file_fmt}{page:02d}.pdf"
            local_progress.update(
                task_id,
                filename=f"{target_date} 第{page:02d}页",
                refresh=True
            )

            if is_new:
                node_url = f"http://paper.people.com.cn/rmrb/pc/layout/{new_fmt}/node_{page:02d}.html"
                if not (response := safe_request(node_url, proxies)):
                    continue
                if pdf_matches := re.findall(r'(/attachement.*?\.pdf)', response.text):
                    url = f"http://paper.people.com.cn/rmrb/pc/{pdf_matches[0]}"
                else:
                    logger.error(f"第{page}页链接未找到")
                    continue
            else:
                url = f"http://paper.people.com.cn/rmrb/images/{old_fmt}/rmrb{file_fmt}{page:02d}.pdf"

            if download_pdf(url, filename, temp_dir, local_progress, task_id, proxies):
                success += 1

        if success < total_pages:
            logger.warning(f"成功下载{success}/{total_pages}页")
            return False
        return True

    except KeyboardInterrupt:
        logger.warning("用户终止下载")
        return False
    except Exception as e:
        logger.error(f"下载异常: {str(e)}")
        return False
    finally:
        local_progress.remove_task(task_id)
        if use_local_progress:
            local_progress.stop()


def merge_pdfs(temp_dir: str, output_dir: str) -> bool:
    """使用PyMuPDF合并PDF文件"""
    try:
        # 获取排序后的PDF文件列表
        pdf_files = sorted(
            [f for f in os.listdir(temp_dir) if f.endswith(".pdf")],
            key=lambda x: int(x[-6:-4])  # 从文件名提取页码
        )

        if not pdf_files:
            logger.error("没有找到可合并的文件")
            return False

        # 创建新文档
        doc = fitz.open()

        for filename in pdf_files:
            file_path = os.path.join(temp_dir, filename)

            # 跳过空文件
            if os.path.getsize(file_path) < 1024:
                logger.warning(f"跳过无效文件: {filename}")
                continue

            try:
                src = fitz.open(file_path)
                doc.insert_pdf(src)  # 插入整个文档
                src.close()
            except Exception as e:
                logger.error(f"文件合并失败: {filename} - {str(e)}")
                return False

        # 生成输出文件名
        output_name = f"People's.Daily.{pdf_files[0][4:12]}.pdf"
        output_path = os.path.join(output_dir, output_name)

        # 保存合并后的文档
        doc.save(output_path, deflate=True, garbage=3)
        doc.close()

        logger.info(f"合并成功: {output_path}")
        return True
    except Exception as e:
        logger.critical(f"合并失败: {str(e)}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="人民日报PDF下载工具",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-d", "--date",
        type=str,
        help="指定日期 (格式: YYYYMMDD/YYYY-MM-DD/YYYY-MM/dd)"
    )
    parser.add_argument(
        "-r", "--range",
        type=str,
        help="日期范围 (格式: YYYYMMDD-YYYYMMDD 或 YYYY-MM-DD-YYYY-MM-DD)"
    )
    parser.add_argument(
        "-p","--proxy",
        type=str,
        help="设置代理服务器（格式：协议://地址:端口 如 socks5://127.0.0.1:1080）"
    )
    args = parser.parse_args()

    if args.date and args.range:
        logger.error("不能同时使用 -d 和 -r 参数")
        sys.exit(1)
    if not args.date and not args.range:
        logger.error("必须指定日期参数 (-d) 或日期范围参数 (-r)")
        sys.exit(1)

    # 代理配置处理
    proxies = None
    if args.proxy:
        try:
            parsed = urlparse(args.proxy)
            if not parsed.scheme or not parsed.hostname or not parsed.port:
                raise ValueError("代理格式错误，应为 协议://地址:端口")
            if parsed.scheme.lower() not in ['http', 'https', 'socks5', 'socks5h']:
                raise ValueError(f"不支持的代理协议: {parsed.scheme}")

            proxies = {'http': args.proxy, 'https': args.proxy}

            console.rule("[bold]代理配置[/bold]")
            console.print(f"• 状态：[bold green]已启用[/bold green]")
            console.print(f"• 协议：[cyan]{parsed.scheme.upper()}[/cyan]")
            console.print(f"• 地址：[cyan]{parsed.hostname}[/cyan]:[cyan]{parsed.port}[/cyan]")
            if parsed.username or parsed.password:
                console.print(
                    f"• 认证：用户[cyan]{parsed.username or '无'}[/cyan] 密码[cyan]{'*' * 3 if parsed.password else '无'}[/cyan]")
        except Exception as e:
            logger.error(f"代理配置错误: {str(e)}")
            sys.exit(1)
    else:
        console.rule("[bold]代理配置[/bold]")
        console.print("• 状态：[bold yellow]未启用[/bold yellow]")

    temp_dir, output_dir = init_environment()
    console.rule("[bold cyan]人民日报PDF下载工具[/bold cyan]")
    console.print("• 程序作者：[bold magenta]XHSecurity[/bold magenta]", style="bold magenta")
    console.print(f"• 程序版本: [bold yellow]V1.0（增强版）[/bold yellow]", style="bold yellow")
    console.print("• 更新日期: [bold green]2025年04月08日[/bold green]", style="bold green")
    console.print("• 程序用途: [bold purple]下载人民日报PDF报纸[/bold purple]", style="bold purple")
    console.print("• 免责声明: [bold red]该程序仅用于学习和研究用途，任何非法用途与作者无关。[/bold red]", style="bold red")

    try:
        if args.range:
            date_list = validate_date_range(args.range)
            total_days = len(date_list)
            logger.info(f"准备下载 {total_days} 天的报纸")

            with Progress(
                    TextColumn("[bold cyan]{task.description}"),
                    BarColumn(bar_width=None),
                    "[progress.percentage]{task.percentage:>3.0f}%",
                    "•",
                    TimeRemainingColumn(),
                    console=console,
                    refresh_per_second=10
            ) as main_progress:
                main_task = main_progress.add_task(
                    "批量下载进度",
                    total=total_days,
                    visible=True
                )

                for idx, date_obj in enumerate(date_list, 1):
                    current_date = date_obj.strftime("%Y-%m-%d")
                    target_date = date_obj.strftime("%Y-%m/%d")

                    main_progress.update(
                        main_task,
                        description=f"处理 {current_date} ({idx}/{total_days})",
                        advance=1,
                        refresh=True
                    )

                    try:
                        current_temp_dir, _ = init_environment()

                        download_success = download_edition(
                            target_date,
                            current_temp_dir,
                            output_dir,
                            progress=main_progress,
                            main_task=main_task,
                            proxies=proxies
                        )

                        if download_success:
                            merge_success = merge_pdfs(current_temp_dir, output_dir)
                            if not merge_success:
                                logger.error(f"{current_date} 合并失败")
                        else:
                            logger.warning(f"{current_date} 下载未完成")

                    except Exception as e:
                        logger.error(f"{current_date} 处理失败: {str(e)}")
                    finally:
                        shutil.rmtree(current_temp_dir, ignore_errors=True)

            logger.info("🎉 批量下载完成！")

        else:
            target_date = validate_date(args.date)
            logger.info(f"目标日期: {target_date}")
            download_success = download_edition(target_date, temp_dir, output_dir, proxies=proxies)
            if download_success:
                merge_success = merge_pdfs(temp_dir, output_dir)
                if not merge_success:
                    logger.error("合并失败")
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.info("🎉 任务完成！")

    except KeyboardInterrupt:
        logger.warning("用户终止操作")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"程序异常: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()