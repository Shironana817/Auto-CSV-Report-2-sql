import os
import pandas as pd
import mysql.connector
from tkinter import *
from tkinter import filedialog
from threading import Timer

# 设置全局变量
db_connection = None

# 连接MySQL数据库
def connect_db(ip, user, password):
    global db_connection
    try:
        db_connection = mysql.connector.connect(
            host=ip,
            user=user,
            password=password,
            database='G5'
        )
        print("数据库连接成功！")
    except mysql.connector.Error as err:
        print(f"数据库连接失败: {err}")

# 创建表（根据文件名和CSV第一行）
def create_table_from_filename(file_path, df):
    table_name = os.path.splitext(os.path.basename(file_path))[0]  # 从文件路径提取文件名并去掉扩展名
    print(f"创建表: {table_name}")

    # 处理列名，使用 DataFrame 的第一行作为列名
    columns = df.columns.tolist()
    

    # 处理列名，避免空列名或无效列名（如 'nan'）
    #columns = [f'column{i+1}' if col.strip() == '' or col.lower() == 'nan' else col for i, col in enumerate(columns)]

    # 动态生成列名
    columns = [col if col != 'nan' and col.strip() else f'n{i+1}' for i, col in enumerate(columns)]
    columns_def = ', '.join([f'`{col}` VARCHAR(255)' for col in columns])
    
    

    # 创建表的SQL语句
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{table_name}` (
        id INT AUTO_INCREMENT PRIMARY KEY,
        {columns_def}
    );
    """
    cursor = db_connection.cursor()
    cursor.execute(create_table_sql)
    db_connection.commit()
    cursor.close()
    print(f"表 {table_name} 创建成功！")
    

# 上传CSV文件到数据库
def upload_csv(file_path):
    if not db_connection:
        print("数据库连接未建立！")
        return

    try:
        # 读取CSV文件，跳过第一行
        df = pd.read_csv(file_path, encoding='gb2312', skiprows=1)  #跳过前几行请根据实际需要调整

        # 填充第一列的前4行为 'time'
        df.iloc[:3, 0] = 'time'
        # 获取列名
        columns = df.columns.tolist()
        columns[0] = 'time'


        for i, col in enumerate(columns):
            if 'Unnamed:' in col:  # 检查列名是否为空
               columns[i] = f'n{i+1}'  # 填充为 n1, n2, n3, ...
        df.columns = columns
        df = df.fillna('$')
        

        
        # 使用 DataFrame 的第一行作为列名
       
        # 根据处理后的 DataFrame 创建表
        create_table_from_filename(file_path, df)

        table_name = os.path.splitext(os.path.basename(file_path))[0]  # 获取文件名作为表名
        #columns = ', '.join([f'`{col}`' for col in df.columns])  # 动态列名
        columns = ', '.join([f'`{col}`' for col in df.columns])
        
        placeholders = ', '.join(['%s'] * len(df.columns))  # 插入数据的占位符

        # 创建 SQL 插入语句
        sql = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
        
        
        cursor = db_connection.cursor()
        
        cursor.executemany(sql, df.values.tolist())  # 批量插入
        print(columns)
        db_connection.commit()
        cursor.close()
        
        print(f"{file_path} 上传成功！")
        
        # 删除已上传的文件
        os.remove(file_path)
        print(f"{file_path} 已删除")
    except Exception as e:
        print(f"上传文件时出错: {e}")

# 定时任务，每5分钟上传一次CSV文件
def start_upload(directory):
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            upload_csv(file_path)
    Timer(300, start_upload, [directory]).start()  # 每300秒执行一次，可更具实际需要调整

# GUI界面
def browse_folder():
    folder_selected = filedialog.askdirectory()
    folder_entry.delete(0, END)
    folder_entry.insert(0, folder_selected)

def start():
    ip = ip_entry.get()
    user = user_entry.get()
    password = password_entry.get()
    folder_path = folder_entry.get()

    # 输入验证
    if not ip or not user or not password or not folder_path:
        print("请填写所有字段！")
        return
    
    connect_db(ip, user, password)
    start_upload(folder_path)

# 主程序
root = Tk()
root.title("CSV上传到MySQL")

# 数据库连接
Label(root, text="数据库IP地址").grid(row=0, column=0)
ip_entry = Entry(root)
ip_entry.insert(0, "0.0.0.0")
ip_entry.grid(row=0, column=1)

Label(root, text="数据库用户名").grid(row=1, column=0)
user_entry = Entry(root)
user_entry.insert(0, "root")
user_entry.grid(row=1, column=1)

Label(root, text="数据库密码").grid(row=2, column=0)
password_entry = Entry(root, show="*")

password_entry.grid(row=2, column=1)

# 文件夹选择
Label(root, text="选择CSV文件夹").grid(row=3, column=0)
folder_entry = Entry(root)
folder_entry.grid(row=3, column=1)
Button(root, text="浏览", command=browse_folder).grid(row=3, column=2)

# 启动按钮
Button(root, text="启动", command=start).grid(row=4, column=1)

root.mainloop()
