"""
人民日报PDF下载工具（修复版）
修复内容：
1. 解决多进度条实例冲突问题
2. 优化批量下载进度显示
3. 增强异常处理机制
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

import requests
import PyPDF2
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
    temp_dir = os.path.join(BASE_DIR, "temp_part")

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
        # 统一去除可能的分隔符
        clean_str = re.sub(r"[^0-9]", "", range_str)
        if len(clean_str) != 16:
            raise ValueError("无效的日期范围格式")

        # 解析开始和结束日期
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

        # 生成日期列表
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


def safe_request(url: str) -> Optional[requests.Response]:
    """带重试机制的请求函数"""
    for _ in range(MAX_RETRIES):
        try:
            response = requests.get(
                url,
                headers=HEADERS,
                timeout=TIMEOUT,
                verify=False,
                allow_redirects=False
            )
            if response.status_code == 200:
                return response
            if response.status_code == 404:
                return None
        except Exception as e:
            logger.warning(f"请求失败: {url} - {str(e)}")
            time.sleep(1)
    return None


def get_page_info(old_url: str, new_url: str) -> Tuple[int, bool]:
    """获取页数并判断版本"""
    # 尝试新版网站
    if response := safe_request(new_url):
        if (pages := len(re.findall(r'pageLink', response.text))) > 0:
            return pages, True

    # 回退旧版网站
    if response := safe_request(old_url):
        return len(re.findall(r'nbs', response.text)), False

    logger.error("无法获取报纸信息，请检查网络或日期")
    sys.exit(1)


def download_pdf(url: str, filename: str, temp_dir: str,
                 progress: Progress, task_id: TaskID) -> bool:
    """带进度条和重试的下载函数"""
    for retry in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, stream=True, timeout=30)
            response.raise_for_status()

            # 检查内容类型
            if 'application/pdf' not in response.headers.get('Content-Type', ''):
                logger.warning(f"非PDF内容: {url}")
                return False

            total_size = int(response.headers.get('content-length', 0))
            file_path = os.path.join(temp_dir, filename)

            progress.update(task_id, total=total_size, visible=True)

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        progress.update(task_id, advance=len(chunk))

            if os.path.getsize(file_path) >= 1024:  # 至少1KB
                return True
        except Exception as e:
            logger.warning(f"下载失败 ({retry + 1}/{MAX_RETRIES}): {filename} - {str(e)}")
            time.sleep(1)

    return False


def download_edition(target_date: str, temp_dir: str, output_dir: str,
                     progress: Optional[Progress] = None,
                     main_task: Optional[TaskID] = None) -> bool:
    """主下载流程（支持外部进度条）"""
    old_fmt, file_fmt, new_fmt = parse_date(target_date)
    output_path = os.path.join(output_dir, f"People's.Daily.{file_fmt}.pdf")

    if os.path.exists(output_path):
        logger.info(f"文件已存在: {os.path.basename(output_path)}")
        return True

    # 构造封面URL
    old_cover = f"http://paper.people.com.cn/rmrb/html/{old_fmt}/nbs.D110000renmrb_01.htm"
    new_cover = f"http://paper.people.com.cn/rmrb/pc/layout/{new_fmt}/node_01.html"

    # 获取页数和版本信息
    total_pages, is_new = get_page_info(old_cover, new_cover)
    logger.info(f"检测到{target_date}共{total_pages}页 ({'新版' if is_new else '旧版'})")

    # 进度条管理
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

        # 添加下载任务
        task_id = local_progress.add_task(
            "downloading",
            filename=f"{target_date} 初始化...",
            total=0,
            visible=True
        )

        # 更新主任务描述
        if main_task and progress:
            progress.update(
                main_task,
                description=f"[cyan]下载 {target_date}",
                refresh=True
            )

        for page in range(1, total_pages + 1):
            # 检查用户中断
            if main_task and progress and progress.tasks[main_task].finished:
                raise KeyboardInterrupt()

            filename = f"rmrb{file_fmt}{page:02d}.pdf"
            local_progress.update(
                task_id,
                filename=f"{target_date} 第{page:02d}页",
                refresh=True
            )

            # 构造下载链接
            if is_new:
                node_url = f"http://paper.people.com.cn/rmrb/pc/layout/{new_fmt}/node_{page:02d}.html"
                if not (response := safe_request(node_url)):
                    continue
                if pdf_matches := re.findall(r'(/attachement.*?\.pdf)', response.text):
                    url = f"http://paper.people.com.cn/rmrb/pc/{pdf_matches[0]}"
                else:
                    logger.error(f"第{page}页链接未找到")
                    continue
            else:
                url = f"http://paper.people.com.cn/rmrb/images/{old_fmt}/rmrb{file_fmt}{page:02d}.pdf"

            if download_pdf(url, filename, temp_dir, local_progress, task_id):
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


def merge_pdfs(temp_dir: str, output_dir: str):
    """合并PDF并校验完整性"""
    try:
        pdf_files = sorted(
            [f for f in os.listdir(temp_dir) if f.endswith(".pdf")],
            key=lambda x: int(x[-6:-4])
        )

        if not pdf_files:
            logger.error("没有找到可合并的文件")
            return False

        merger = PyPDF2.PdfMerger()
        valid_files = []
        for f in pdf_files:
            path = os.path.join(temp_dir, f)
            if os.path.getsize(path) < 1024:
                logger.warning(f"跳过无效文件: {f}")
                continue
            try:
                merger.append(path)
                valid_files.append(f)
            except PyPDF2.errors.PdfReadError:
                logger.error(f"文件损坏: {f}")
                return False

        if len(valid_files) == 0:
            logger.error("没有有效的PDF文件可供合并")
            return False

        output_name = f"People's.Daily.{valid_files[0][4:12]}.pdf"
        output_path = os.path.join(output_dir, output_name)
        merger.write(output_path)
        merger.close()
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
    args = parser.parse_args()

    # 参数检查
    if args.date and args.range:
        logger.error("不能同时使用 -d 和 -r 参数")
        sys.exit(1)
    if not args.date and not args.range:
        logger.error("必须指定日期参数 (-d) 或日期范围参数 (-r)")
        sys.exit(1)

    temp_dir, output_dir = init_environment()
    console.rule("[bold cyan]人民日报PDF下载工具[/bold cyan]")
    console.print(f"版本: 2025.4 (修复版)", style="bold yellow")

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

                    # 更新主进度条
                    main_progress.update(
                        main_task,
                        description=f"处理 {current_date} ({idx}/{total_days})",
                        advance=1,
                        refresh=True
                    )

                    try:
                        current_temp_dir, _ = init_environment()

                        # 执行下载
                        download_success = download_edition(
                            target_date,
                            current_temp_dir,
                            output_dir,
                            progress=main_progress,
                            main_task=main_task
                        )

                        # 合并PDF
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
            download_success = download_edition(target_date, temp_dir, output_dir)
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