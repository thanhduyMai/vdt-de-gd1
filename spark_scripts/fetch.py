import os
import sys
import time
import stat
try:
    import paramiko
    from airflow.hooks.base import BaseHook
except ImportError as e:
    print(f' Lỗi: Chưa cài đặt đủ thư viện ({e})')
    sys.exit(1)

def run_fetch():
    conn_id = 'mdt_sftp_server'
    print(f' Đang đọc cấu hình {conn_id} từ Airflow...')
    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception as e:
        print(f' Không tìm thấy Connection: {e}')
        sys.exit(1)

    host = conn.host
    port = conn.port if conn.port else 2222
    user = conn.login
    password = conn.password

    remote_base_dir = '/home/vdt-data-de'
    local_base_dir = '/tmp/vdt_data'
    
    # Danh sách 3 thư mục bạn vừa ls ra
    folders = ['cell-info', 'mdt']
    
    # Tạo thư mục gốc dưới local
    os.makedirs(local_base_dir, exist_ok=True)

    print(f' Bắt đầu kéo dữ liệu từ {user}@{host}:{port}...')
    
    try:
        transport = paramiko.Transport((host, port))
        transport.connect(username=user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        
        total_files = 0
        total_time = 0.0
        
        for folder in folders:
            remote_dir = f'{remote_base_dir}/{folder}'
            local_dir = f'{local_base_dir}/{folder}'
            os.makedirs(local_dir, exist_ok=True)
            
            print(f'\n  {remote_dir}')
            try:
                files = sftp.listdir(remote_dir)
            except Exception as e:
                print(f'    Lỗi (Có thể thư mục không tồn tại): {e}')
                continue
                
            if not files:
                print('   -> Thư mục trống.')
                continue
                
            for file_name in files:
                remote_file = f'{remote_dir}/{file_name}'
                local_file = f'{local_dir}/{file_name}'
                
                # Bỏ qua nếu nó là thư mục con, chỉ tải file
                file_attr = sftp.stat(remote_file)
                if stat.S_ISDIR(file_attr.st_mode):
                    continue

                print(f'     {file_name}... ', end='', flush=True)
                
                # BẤM GIỜ
                start_time = time.time()
                sftp.get(remote_file, local_file)
                end_time = time.time()
                
                duration = end_time - start_time
                total_time += duration
                total_files += 1
                
                print(f' ({duration:.3f}s)')
        
        sftp.close()
        transport.close()
        
        print('\n=======================================================')
        print(' BÁO CÁO HIỆU NĂNG TẢI DỮ LIỆU SFTP')
        print('=======================================================')
        print(f'Tổng số file đã tải  : {total_files} file')
        print(f'Tổng thời gian tải   : {total_time:.3f} giây')
        
        if total_files > 0:
            avg_time = total_time / total_files
            print(f' Tốc độ trung bình  : {avg_time:.3f} giây / file')
        else:
            print(' Tốc độ trung bình  : N/A (Không tải file nào)')
            
        print(f' Dữ liệu lưu tại    : {local_base_dir}/')
        print('=======================================================\n')
        
    except Exception as e:
        print(f'\n LỖI TRONG QUÁ TRÌNH TẢI: {e}')

if __name__ == '__main__':
    run_fetch()
