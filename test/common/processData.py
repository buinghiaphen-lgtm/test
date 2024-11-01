from common.logger import logger
from common.tools import Excel,DB
from datetime import datetime,timedelta
from setting import TEST_DATA_PATH
from common import get_mysql_data


def insert_into_ticket(current_time):
    """更新/插入 ticket表数据"""
    logger.info("正在准备ticket数据中......")
    ticket_datalist = getDataFromExcelSheet("ticket", current_time)
    #partion_list = getPartionList("ticket")
    # with DB() as db:
    #     db.add_partition('ticket', partion_list)
    sql_insert_ticket = 'REPLACE ticket(draw_id,ticket_no,clerk_id,ticket_pwd,sale_time,chances,selection,multiple,bno,eno,transaction_id,term_id) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    get_mysql_data.MysqlDb().executemany_db_val(sql_insert_ticket, ticket_datalist)
    logger.info("ticket数据准备完毕......")

def insert_into_ticket_page(current_time):
    """分页更新/插入 ticket表数据
    分页推送
    """
    logger.info("正在准备ticket_page数据中......")
    ticket_page_datalist = getDataFromExcelSheet("ticket_page", current_time)
    # partion_list = getPartionList("ticket_page")
    # with DB() as db:
    #     db.add_partition('ticket', partion_list)
    sql_insert_ticket = "REPLACE INTO ticket(draw_id,ticket_no,clerk_id,ticket_pwd,sale_time,chances,selection," \
                        "multiple,bno,eno,transaction_id,term_id) " \
                        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(sql_insert_ticket, ticket_page_datalist)
    logger.info("ticket_page数据准备完毕......")

def insert_into_cancel_ticket(current_time):
    """更新/插入 cancel_ticket表数据"""
    logger.info("正在准备cancel_ticket数据中......")
    cancel_ticket_datalist = getDataFromExcelSheet("cancel_ticket", current_time)
    sql_insert_cancel_ticket = "REPLACE INTO cancel_ticket(draw_id,ticket_no,clerk_id," \
                               "ticket_pwd,sale_time,chances,selection,multiple,cancel_type,cancel_status,cancel_time," \
                               "cancel_operator_id,bno,eno,transaction_id,cancel_reason_id) " \
                               "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(sql_insert_cancel_ticket, cancel_ticket_datalist)
    logger.info("cancel_ticket数据准备完毕......")

def insert_into_cancel_ticket_page(current_time):
    """分页更新/插入 cancel_ticket表数据"""
    logger.info("正在准备cancel_ticket_page数据中......")
    cancel_ticket_datalist = getDataFromExcelSheet("cancel_ticket_page", current_time)
    sql_insert_cancel_ticket = "REPLACE INTO cancel_ticket(draw_id,ticket_no,clerk_id," \
                               "ticket_pwd,sale_time,chances,selection,multiple,cancel_type,cancel_status,cancel_time," \
                               "cancel_operator_id,bno,eno,transaction_id,cancel_reason_id) " \
                               "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(sql_insert_cancel_ticket, cancel_ticket_datalist)
    logger.info("cancel_ticket_page数据准备完毕......")

def insert_into_undo_ticket(current_time):
    """更新/插入 undo_ticket表数据"""
    logger.info("正在准备undo_ticket数据中......")
    undo_ticket_datalist = getDataFromExcelSheet("undo_ticket", current_time)
    sql_insert_undo_ticket = "REPLACE INTO undo_ticket(draw_id,ticket_no,undo_time,term_id,clerk_id," \
                             "ticket_pwd,sale_time,chances,selection,multiple,undo_reason_id,undo_reason,undo_fail_reason_id," \
                             "bno,eno,transaction_id,undo_status) " \
                             "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(sql_insert_undo_ticket, undo_ticket_datalist)
    logger.info("undo_ticket数据准备完毕......")

def insert_into_undo_ticket_page(current_time):
    """分页更新/插入 undo_ticket表数据"""
    logger.info("正在准备undo_ticket_page数据中......")
    undo_ticket_datalist = getDataFromExcelSheet("undo_ticket_page", current_time)
    sql_insert_undo_ticket = "REPLACE INTO undo_ticket(draw_id,ticket_no,undo_time,term_id,clerk_id," \
                             "ticket_pwd,sale_time,chances,selection,multiple,undo_reason_id,undo_reason,undo_fail_reason_id," \
                             "bno,eno,transaction_id,undo_status) " \
                             "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(sql_insert_undo_ticket, undo_ticket_datalist)
    logger.info("undo_ticket_page数据准备完毕......")

def insert_into_win_ticket(current_time):
    """更新/插入 win_ticket表数据"""
    logger.info("正在准备win_ticket数据中......")
    win_ticket_datalist = getDataFromExcelSheet("win_ticket", current_time)
    # partion_list = getPartionList("win_ticket")
    # with DB() as db:
    #     db.add_partition('win_ticket', partion_list)
    sql_insert_win_ticket = "REPLACE INTO win_ticket(draw_id, ticket_no, win_prz_lvl, clerk_id, ticket_pwd, sale_time, " \
                            "win_time, paid_time, chances, selection, multiple, prz_cnt, prz_amt, tax_amt, paid_type, " \
                            "paid_operator_id, withdraw_amt, bno, eno, transaction_id, payment_type) " \
                            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(sql_insert_win_ticket, win_ticket_datalist)
    logger.info("win_ticket数据准备完毕......")

def insert_into_win_ticket_page(current_time):
    """分页更新/插入 win_ticket表数据"""
    logger.info("正在准备win_ticket_page数据中......")
    win_ticket_datalist = getDataFromExcelSheet("win_ticket_page", current_time)
    # partion_list = getPartionList("win_ticket_page")
    # with DB() as db:
    #     db.add_partition('win_ticket', partion_list)
    sql_insert_win_ticket = "REPLACE INTO win_ticket(draw_id, ticket_no, win_prz_lvl, clerk_id, ticket_pwd, sale_time, " \
                            "win_time, paid_time, chances, selection, multiple, prz_cnt, prz_amt, tax_amt, paid_type, " \
                            "paid_operator_id, withdraw_amt, bno, eno, transaction_id, payment_type) " \
                            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(sql_insert_win_ticket, win_ticket_datalist)
    logger.info("win_ticket_page数据准备完毕......")

def insert_into_paid_ticket(current_time):
    """更新/插入 paid_ticket表数据"""
    logger.info("正在准备paid_ticket数据中......")
    win_ticket_datalist = getDataFromExcelSheet("paid_ticket", current_time)
    # partion_list = getPartionList("paid_ticket")
    # with DB() as db:
    #     db.add_partition('win_ticket', partion_list)
    sql_insert_win_ticket = "REPLACE INTO win_ticket(draw_id, ticket_no, win_prz_lvl, clerk_id, ticket_pwd, sale_time, " \
                            "win_time, paid_time, chances, selection, multiple, prz_cnt, prz_amt, tax_amt, paid_type, " \
                            "paid_operator_id, withdraw_amt, bno, eno, transaction_id, payment_type) " \
                            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(sql_insert_win_ticket, win_ticket_datalist)
    logger.info("paid_ticket数据准备完毕......")

def insert_into_paid_ticket_page(current_time):
    """分页更新/插入 paid_ticket表数据"""
    logger.info("正在准备paid_ticket数据中......")
    win_ticket_datalist = getDataFromExcelSheet("paid_ticket_page", current_time)
    # partion_list = getPartionList("paid_ticket_page")
    # with DB() as db:
    #     db.add_partition('win_ticket', partion_list)
    sql_insert_win_ticket = "REPLACE INTO win_ticket(draw_id, ticket_no, win_prz_lvl, clerk_id, ticket_pwd, sale_time, " \
                            "win_time, paid_time, chances, selection, multiple, prz_cnt, prz_amt, tax_amt, paid_type, " \
                            "paid_operator_id, withdraw_amt, bno, eno, transaction_id, payment_type) " \
                            "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(sql_insert_win_ticket, win_ticket_datalist)
    logger.info("paid_ticket_page数据准备完毕......")

def insert_into_win_ticket_prize(current_time):
    """更新/插入 win_ticket_prize表数据"""
    logger.info("正在准备win_ticket_prize数据中......")
    win_ticket_prize_datalist = getDataFromExcelSheet("win_ticket_prize", current_time)
    sql_insert_win_ticket_prize = "REPLACE INTO win_ticket_prize(draw_id,ticket_no,win_prz_lvl,clerk_id,ticket_amt,eno,prz_cnt," \
                                  "prz_amt,paid_type,paid_time,paid_operator_id,prize_amt,prize_cnt,active_id,prize_tax) " \
                                  "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(
        sql_insert_win_ticket_prize, win_ticket_prize_datalist)
    logger.info("win_ticket_prize数据准备完毕......")

def insert_into_win_ticket_prize_page(current_time):
    """分页更新/插入 win_ticket_prize表数据"""
    logger.info("正在准备win_ticket_prize_page数据中......")
    win_ticket_prize_datalist = getDataFromExcelSheet("win_ticket_prize_page", current_time)
    sql_insert_win_ticket_prize = "REPLACE INTO win_ticket_prize(draw_id,ticket_no,win_prz_lvl,clerk_id,ticket_amt,eno,prz_cnt," \
                                  "prz_amt,paid_type,paid_time,paid_operator_id,prize_amt,prize_cnt,active_id,prize_tax) " \
                                  "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(
        sql_insert_win_ticket_prize, win_ticket_prize_datalist)
    logger.info("win_ticket_prize_page数据准备完毕......")

"""增加game_draw表数据"""
def insert_into_game_draw(current_time):
    """更新/插入 game_draw表数据"""
    logger.info("正在准备game_draw数据中......")
    game_draw_datalist = getDataFromExcelSheet("game_draw", current_time)
    sql_insert_game_draw = "REPLACE INTO game_draw(draw_id,paid_end_time,game_id,draw_year,draw_no,sale_begin_time,sale_end_time," \
                                  "draw_status,draw_result,draw_type,prize_calculation,suspend_status,draw_time,paid_begin_time,game_type) " \
                                  "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    get_mysql_data.MysqlDb().executemany_db_val(
        sql_insert_game_draw, game_draw_datalist)
    logger.info("game_draw数据准备完毕......")

def delete_from_ticket():
    """删除 ticket表数据及分区"""
    logger.info("正在删除ticket测试数据中......")
    sql_delete_ticket = "DELETE FROM ticket WHERE ticket_id=%s AND draw_id=%s AND ticket_no=%s;;"
    ticket_list = getDataDeleteList("ticket")
    get_mysql_data.MysqlDb().executemany_db_val(
        sql_delete_ticket, ticket_list)
    # sql_drop_partion_ticket = "ALTER TABLE ticket DROP PARTITION p%s;"
    # partion_ticket = getPartionList("ticket", "delete")
    # get_mysql_data.MysqlDb().executemany_db_val( sql_drop_partion_ticket, partion_ticket)
    logger.info("ticket测试数据删除完毕......")

def delete_from_table():
    """根据模糊查询删除 ticket、cancel_ticket、undo_ticket、win_ticket、win_ticket_prize表数据"""
    logger.info('开始删除测试数据')
    ticket_sql = 'DELETE FROM ticket WHERE ticket_no LIKE "9999999999%";'
    cancel_ticket_sql = 'DELETE FROM cancel_ticket WHERE ticket_no LIKE "9999999999%";'
    undo_ticket_sql = 'DELETE FROM undo_ticket WHERE ticket_no LIKE "9999999999%";'
    win_ticket_sql = 'DELETE FROM win_ticket WHERE ticket_no LIKE "9999999999%";'
    win_ticket_prize_sql = 'DELETE FROM win_ticket_prize WHERE ticket_no LIKE "9999999999%";'
    with DB() as db:
        logger.info('正在删除ticket数据')
        db.execute(ticket_sql)
        logger.info('正在删除cancel_ticket数据')
        db.execute(cancel_ticket_sql)
        logger.info('正在删除undo_ticket数据')
        db.execute(undo_ticket_sql)
        logger.info('正在删除win_ticket数据')
        db.execute(win_ticket_sql)
        logger.info('正在删除win_ticket_prize数据')
        db.execute(win_ticket_prize_sql)
    logger.info('测试数据删除完毕')

def delete_from_ticket_page():
    """删除 ticket表分页测试数据"""
    logger.info("正在删除ticket_page测试数据中......")
    sql_delete_ticket = "DELETE FROM ticket WHERE ticket_id=%s AND draw_id=%s AND ticket_no=%s;;"
    ticket_list = getDataDeleteList("ticket_page")
    get_mysql_data.MysqlDb().executemany_db_val(
        sql_delete_ticket, ticket_list)
    # sql_drop_partion_ticket = "ALTER TABLE ticket DROP PARTITION p%s;"
    # partion_ticket = getPartionList("ticket_page", "delete")
    # get_mysql_data.MysqlDb().executemany_db_val( sql_drop_partion_ticket, partion_ticket)
    logger.info("ticket_page测试数据删除完毕......")

def delete_from_cancel_ticket():
    """删除 cancel_ticket表测试数据"""
    logger.info("正在删除cancel_ticket测试数据中......")
    sql_delete_cancel_ticke = "DELETE FROM cancel_ticket WHERE cancel_id = %s;"
    cancel_id_list = getDataDeleteList("cancel_ticket")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_cancel_ticke, cancel_id_list)
    logger.info("cancel_ticket测试数据删除完毕......")

def delete_from_cancel_ticket_page():
    """删除 cancel_ticket表分页测试数据"""
    logger.info("正在删除cancel_ticket_page数据......")
    sql_delete_cancel_ticke = "DELETE FROM cancel_ticket WHERE cancel_id = %s;"
    cancel_id_list = getDataDeleteList("cancel_ticket_page")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_cancel_ticke, cancel_id_list)
    logger.info("cancel_ticket_page数据删除完毕......")

def delete_from_undo_ticket():
    """删除 undo_ticket表测试数据"""
    logger.info("正在删除undo_ticket测试数据中......")
    sql_delete_undo_ticket = "DELETE FROM undo_ticket WHERE undo_id = %s ;"
    undo_ticket_list = getDataDeleteList("undo_ticket")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_undo_ticket, undo_ticket_list)
    logger.info("undo_ticket测试数据删除完毕......")

def delete_from_undo_ticket_page():
    """删除 undo_ticket表分页测试数据"""
    logger.info("正在删除undo_ticket_page测试数据中......")
    sql_delete_undo_ticket = "DELETE FROM undo_ticket WHERE undo_id = %s ;"
    undo_ticket_list = getDataDeleteList("undo_ticket_page")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_undo_ticket, undo_ticket_list)
    logger.info("undo_ticket_page测试数据删除完毕......")

def delete_from_win_ticket():
    """删除 win_ticket表测试数据及分区"""
    logger.info("正在删除win_ticket测试数据中......")
    sql_delete_win_ticket = "DELETE FROM win_ticket WHERE draw_id=%s AND ticket_no=%s AND ticket_id=%s AND win_prz_lvl=%s ;"
    win_ticket_list = getDataDeleteList("win_ticket")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_win_ticket, win_ticket_list)
    # sql_drop_partion_win_ticket = "ALTER TABLE win_ticket DROP PARTITION p%s;"
    # partion_ticket = getPartionList("win_ticket", "delete")
    # res = get_mysql_data.MysqlDb().executemany_db_val( sql_drop_partion_win_ticket, partion_ticket)
    logger.info("win_ticket测试数据删除完毕......")

def delete_from_win_ticket_page():
    """删除 win_ticket表分页测试数据及分区"""
    logger.info("正在删除win_ticket_page测试数据中......")
    sql_delete_win_ticket_page = "DELETE FROM win_ticket WHERE draw_id=%s AND ticket_no=%s AND ticket_id=%s AND win_prz_lvl=%s ;"
    win_ticket_page_list = getDataDeleteList("win_ticket_page")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_win_ticket_page, win_ticket_page_list)
    # sql_drop_partion_win_ticket_page = "ALTER TABLE win_ticket DROP PARTITION p%s;"
    # partion_ticket = getPartionList("win_ticket_page", "delete")
    # get_mysql_data.MysqlDb().executemany_db_val( sql_drop_partion_win_ticket_page, partion_ticket)
    logger.info("win_ticket_page测试数据删除完毕......")

def delete_from_paid_ticket():
    """删除 win_ticket表测试数据及分区"""
    logger.info("正在删除paid_ticket测试数据中......")
    sql_delete_paid_ticket = "DELETE FROM win_ticket WHERE draw_id=%s AND ticket_no=%s AND ticket_id=%s AND win_prz_lvl=%s ;"
    paid_ticket_list = getDataDeleteList("paid_ticket")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_paid_ticket, paid_ticket_list)
    # sql_drop_partion_paid_ticket = "ALTER TABLE win_ticket DROP PARTITION p%s;"
    # partion_ticket = getPartionList("paid_ticket", "delete")
    # get_mysql_data.MysqlDb().executemany_db_val( sql_drop_partion_paid_ticket, partion_ticket)
    logger.info("paid_ticket测试数据删除完毕......")

def delete_from_paid_ticket_page():
    """删除 win_ticket表测试数据及分区"""
    logger.info("正在删除paid_ticket_page测试数据中......")
    sql_delete_paid_ticket_page = "DELETE FROM win_ticket WHERE draw_id=%s AND ticket_no=%s AND ticket_id=%s AND win_prz_lvl=%s ;"
    paid_ticket_page_list = getDataDeleteList("paid_ticket_page")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_paid_ticket_page, paid_ticket_page_list)
    # sql_drop_partion_paid_ticket_page = "ALTER TABLE win_ticket DROP PARTITION p%s;"
    # partion_ticket_page = getPartionList("paid_ticket_page", "delete")
    # get_mysql_data.MysqlDb().executemany_db_val( sql_drop_partion_paid_ticket_page, partion_ticket_page)
    logger.info("paid_ticket_page测试数据删除完毕......")

def delete_from_win_ticket_prize():
    """删除win_ticket_prize表测试数据"""
    logger.info("正在删除win_ticket_prize测试数据中......")
    sql_delete_win_ticket_prize = "DELETE FROM win_ticket_prize WHERE draw_id=%s AND ticket_no=%s AND win_prz_lvl=%s;"
    win_ticket_prize_list = getDataDeleteList("win_ticket_prize")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_win_ticket_prize, win_ticket_prize_list)
    logger.info("win_ticket_prize测试数据删除完毕......")

def delete_from_win_ticket_prize_page():
    """删除win_ticket_prize表分页测试数据"""
    logger.info("正在删除win_ticket_prize_page测试数据中......")
    sql_delete_win_ticket_prize_page = "DELETE FROM win_ticket_prize WHERE draw_id=%s AND ticket_no=%s AND win_prz_lvl=%s;"
    win_ticket_prize_page_list = getDataDeleteList("win_ticket_prize_page")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_win_ticket_prize_page, win_ticket_prize_page_list)
    logger.info("win_ticket_prize_page测试数据删除完毕......")

def delete_from_game_draw():
    """删除game_draw表测试数据"""
    logger.info("正在删除game_draw测试数据中......")
    sql_delete_win_ticket_prize_page = "DELETE FROM game_draw WHERE draw_id=%s ;"
    game_draw_list = getDataDeleteList("game_draw")
    get_mysql_data.MysqlDb().executemany_db_val(sql_delete_win_ticket_prize_page, game_draw_list)
    logger.info("win_ticket_prize_page测试数据删除完毕......")


def select_from_ticket(begin_time, end_time):
    """查询ticket表测试数据
            :return:
            selectResultListList:list[list]
            count：int
    """
    select_sql = f'''
                    SELECT 
                        draw_id,
                        ticket_no,
                        clerk_id,
                        ticket_pwd AS 'password',
                        sale_time,
                        chances,
                        selection,
                        multiple,
                        '' AS 'add_flag',
                        bno,
                        eno,
                        transaction_id
                    FROM 
                        ticket
                    WHERE  
                        sale_time BETWEEN "{begin_time}" AND "{end_time}"
                    and 
                        draw_id in ( select draw_id
                        from game_draw
                        where sale_end_time >= "{begin_time}" and sale_begin_time <= "{end_time}")
                    ORDER BY draw_id ,ticket_no;
    '''
    # 获取的database的数据
    # select_result的数据：listdict格式 [ {字段名1:值1,字段名2:值2, 字段名3:值3}, {字段名1:值11,字段名2:值22, 字段名3:值33}  ]
    # select_descr的数据： [ 字段名1, 字段名2, 字段名3 ]
    select_result, select_descr = get_mysql_data.MysqlDb().select_db_value_desc(select_sql)
    # 将select_result的数据内容的值，做了个str()转换，转换之前判断是不是None，若为None，则为''，数据格式没变，还是listdict格式
    selectResultList = select_result_selection_to_str(select_result, select_descr)
    selectResultListList = extract_values(selectResultList)
    selectResultListList = sorted(selectResultListList, key=lambda x: [x[1]])
    selectResultListList.insert(0, select_descr)
    count = int(len(selectResultListList))
    if count==1:
        return [],0
    return selectResultListList, count

def select_from_cancel_ticket(begin_time, end_time):
    """查询cancel_ticket表测试数据
            :return:
            selectResultListList:list[list]
            count：int
    """
    select_sql = f'''
                SELECT 
                    draw_id,
                    ticket_no,
                    clerk_id,
                    ticket_pwd AS 'password',
                    sale_time,
                    chances,
                    selection,
                    multiple,
                    cancel_type,
                    cancel_time,
                    cancel_operator_id,
                    '' AS 'add_flag',
                    bno,
                    eno,
                    transaction_id,
                    cancel_reason_id AS cancel_reason
                FROM 
                    cancel_ticket
                WHERE 
                    cancel_status = 0 
                AND 
                    cancel_time BETWEEN '{begin_time}' AND '{end_time}' ORDER BY cancel_id;
    '''
    # 获取的database的数据
    # select_result的数据：listdict格式 [ {字段名1:值1,字段名2:值2, 字段名3:值3}, {字段名1:值11,字段名2:值22, 字段名3:值33}  ]
    # select_descr的数据： [ 字段名1, 字段名2, 字段名3 ]
    logger.info(select_sql)
    select_result, select_descr = get_mysql_data.MysqlDb().select_db_value_desc(select_sql)
    logger.info(f'select_result----{select_result}')
    # 将select_result的数据内容的值，做了个str()转换，转换之前判断是不是None，若为None，则为''，数据格式没变，还是listdict格式
    selectResultList = select_result_selection_to_str(select_result, select_descr)
    selectResultListList = extract_values(selectResultList)
    selectResultListList = sorted(selectResultListList, key=lambda x: [x[2],x[3]])
    selectResultListList.insert(0, select_descr)
    count = int(len(selectResultListList))
    if count==1:
        return [],0
    return selectResultListList, count

def select_from_undo_ticket(begin_time, end_time):
    """查询undo_ticket表测试数据
            :return:
            selectResultListList:list[list]
            count：int
    """
    select_sql = f'''
            SELECT 
                ut.draw_id,  
                ut.ticket_no, 
                gd.game_id, 
                1 AS 'game_ver',
                undo_time,
                t.term_no AS 'term_id',
                ut.clerk_id,
                ut.ticket_pwd AS 'password',
                ut.sale_time,
                ut.chances,
                ut.selection,
                ut.multiple,
                1 AS 'sale_draw_cnt',
                0 AS 'paid_draw_cnt',
                1 AS 'total_draw_cnt',
                '' AS 'sale_draw_list',
                '' AS 'paid_draw_list',
                '' AS 'renew_clerk_id',
                '' AS 'old_ticket_no',
                1 AS 'undo_type',
                0 AS 'confirm_flag',
                ut.undo_reason_id AS 'undo_err_id',
                ut.undo_reason AS 'undo_err_desc',
                ut.undo_fail_reason_id AS 'undo_failed_err_id',
                "" AS 'add_flag',
                ut.bno,
                ut.eno,
                ut.transaction_id
            FROM 
                undo_ticket ut
            LEFT JOIN game_draw gd ON
                ut.draw_id = gd.draw_id
            LEFT JOIN term t ON
                ut.term_id = t.term_id
            WHERE
                ut.undo_status = 1
            AND
                undo_time BETWEEN '{begin_time}' AND '{end_time}'
            ORDER BY undo_id;
    '''
    # 获取的database的数据
    # select_result的数据：listdict格式 [ {字段名1:值1,字段名2:值2, 字段名3:值3}, {字段名1:值11,字段名2:值22, 字段名3:值33}  ]
    # select_descr的数据： [ 字段名1, 字段名2, 字段名3 ]
    select_result, select_descr = get_mysql_data.MysqlDb().select_db_value_desc(select_sql)
    logger.info(f'undo_ticket查询结果={select_result}')
    logger.info(f'undo_ticket查询表头={select_descr}')
    # 将select_result的数据内容的值，做了个str()转换，转换之前判断是不是None，若为None，则为''，数据格式没变，还是listdict格式
    selectResultList = select_result_selection_to_str(select_result, select_descr)
    selectResultListList = extract_values(selectResultList)
    selectResultListList = sorted(selectResultListList, key=lambda x: [x[0],x[1]])
    selectResultListList.insert(0, select_descr)
    count = int(len(selectResultListList))
    if count==1:
        return [],0
    return selectResultListList, count

def select_from_win_ticket(begin_time, end_time):
    """查询win_ticket表测试数据
            :return:
            selectResultListList:list[list]
            count：int
    """
    select_sql = f'''
            SELECT 
                draw_id,
                ticket_no,
                win_prz_lvl,
                clerk_id,
                ticket_pwd AS 'password',
                sale_time,
                chances,
                selection,
                multiple,
                CAST(prz_cnt AS CHAR)AS prz_cnt,
                prz_amt,
                tax_amt,
                paid_type,
                paid_time,
                '' AS 'winner_name',
                '' AS 'certificate_type',
                '' AS 'certificate_no',
                paid_operator_id,
                '' AS 'add_flag',
                withdraw_amt,
                bno,
                eno,
                transaction_id,
                '' AS 'winner_id',
                payment_type
            FROM 
                win_ticket
            WHERE 
                win_time BETWEEN '{begin_time}' AND '{end_time}'
            ORDER BY draw_id, ticket_no, win_prz_lvl;
    '''
    # 获取的database的数据
    # select_result的数据：listdict格式 [ {字段名1:值1,字段名2:值2, 字段名3:值3}, {字段名1:值11,字段名2:值22, 字段名3:值33}  ]
    # select_descr的数据： [ 字段名1, 字段名2, 字段名3 ]
    select_result, select_descr = get_mysql_data.MysqlDb().select_db_value_desc(select_sql)
    # 将select_result的数据内容的值，做了个str()转换，转换之前判断是不是None，若为None，则为''，数据格式没变，还是listdict格式
    selectResultList = select_result_selection_to_str(select_result, select_descr)
    selectResultListList = extract_values(selectResultList)
    selectResultListList = sorted(selectResultListList, key=lambda x: [x[0], x[1], x[2]])
    selectResultListList.insert(0, select_descr)
    count = int(len(selectResultListList))
    if count==1:
        return [],0
    return selectResultListList, count

def select_from_paid_ticket(begin_time, end_time):
    """查询win_ticket表分页测试数据
            :return:
            selectResultListList:list[list]
            count：int
    """
    select_sql = f'''
            SELECT 
                draw_id,
                ticket_no,
                win_prz_lvl,
                clerk_id,
                ticket_pwd AS 'password',
                sale_time,
                chances,
                selection,
                multiple,
                CAST(prz_cnt AS CHAR)AS prz_cnt,
                prz_amt,
                tax_amt,
                paid_type,
                paid_time,
                '' AS 'winner_name',
                '' AS 'certificate_type',
                '' AS 'certificate_no',
                paid_operator_id,
                '' AS 'add_flag',
                withdraw_amt,
                bno,
                eno,
                transaction_id,
                '' AS 'winner_id',
                payment_type
            FROM 
                win_ticket
            WHERE 
                paid_time BETWEEN '{begin_time}' AND '{end_time}'
            ORDER BY draw_id, ticket_no, win_prz_lvl;
    '''
    # 获取的database的数据
    # select_result的数据：listdict格式 [ {字段名1:值1,字段名2:值2, 字段名3:值3}, {字段名1:值11,字段名2:值22, 字段名3:值33}  ]
    # select_descr的数据： [ 字段名1, 字段名2, 字段名3 ]
    select_result, select_descr = get_mysql_data.MysqlDb().select_db_value_desc(select_sql)
    # 将select_result的数据内容的值，做了个str()转换，转换之前判断是不是None，若为None，则为''，数据格式没变，还是listdict格式
    selectResultList = select_result_selection_to_str(select_result, select_descr)
    selectResultListList = extract_values(selectResultList)
    selectResultListList = sorted(selectResultListList, key=lambda x: [x[0], x[1], x[2]])
    selectResultListList.insert(0, select_descr)
    count = int(len(selectResultListList))
    if count==1:
        return [],0
    return selectResultListList, count

def select_from_win_ticket_prize(begin_time, end_time):
    """查询win_ticket_prize表测试数据
            :return:
            selectResultListList:list[list]
            count：int
    """
    select_sql = f'''
            SELECT 
                wtp.draw_id,     
                wtp.ticket_no, 
                wtp.win_prz_lvl, 
                wtp.clerk_id,
                wtp.ticket_amt AS 'money',
                wtp.eno,
                wtp.prz_cnt,
                wtp.prz_amt,
                wtp.paid_type,
                wtp.paid_time,
                wtp.paid_operator_id,
                wtp.prize_amt,
                CAST(wtp.prize_cnt AS CHAR)AS prize_cnt,
                wtp.active_id,
                wtp.prize_tax
            FROM
                win_ticket_prize wtp
            WHERE 
                paid_time BETWEEN '{begin_time}' AND '{end_time}'
            ORDER BY draw_id, ticket_no, win_prz_lvl;
    '''
    # 获取的database的数据
    # select_result的数据：listdict格式 [ {字段名1:值1,字段名2:值2, 字段名3:值3}, {字段名1:值11,字段名2:值22, 字段名3:值33}  ]
    # select_descr的数据： [ 字段名1, 字段名2, 字段名3 ]
    logger.info(f'select_sql={select_sql}')
    select_result, select_descr = get_mysql_data.MysqlDb().select_db_value_desc(select_sql)
    logger.info(f'select_result={select_result}')
    # 将select_result的数据内容的值，做了个str()转换，转换之前判断是不是None，若为None，则为''，数据格式没变，还是listdict格式
    selectResultList = select_result_selection_to_str(select_result, select_descr)
    selectResultListList = extract_values(selectResultList)
    selectResultListList = sorted(selectResultListList, key=lambda x: [x[0], x[1], x[2]])
    selectResultListList.insert(0, select_descr)
    count = int(len(selectResultListList))
    if count==1:
        return [],0
    return selectResultListList, count

# [{key1:value1,key2:value2,key3:value3},{key1:value11,key2:value22,key3:value33}]
# 转成 [[value1,value2,value3],[value11,value22,value33]]
def extract_values(listdictionary):
    """数据类型处理
    Args:
        listdictionary [{},{}......]: _数组+字典类型
    :return:
        valuefinal:list[list] :[] _返回数组类型数据
    """
    valuefinal = []
    for dictionary in listdictionary:
        values = []  # 创建一个空数组，用于存储字典的值
        # 遍历字典的所有键，并获取对应的值存储到数组中
        for key in dictionary.keys():
            value = dictionary[key]
            values.append(value)
        valuefinal.append(values)
    return valuefinal  # 返回存储值的数组

def extract_ticket_values_add_title(listdictionary):
    """ticket数据类型处理
    Args:
        listdictionary [{},{}......]: _数组+字典类型
    :return:
        valuefinal:list[list] :[] _返回数组类型数据
    """
    if len(listdictionary)==0:
        return []
    title_list = list(listdictionary[0].keys())
    valuefinal = []
    for dictionary in listdictionary:
        values = []  # 创建一个空数组，用于存储字典的值
        # 遍历字典的所有键，并获取对应的值存储到数组中
        for key in dictionary.keys():
            if dictionary[key] == None:
                dictionary[key] = ''
            value = str(dictionary[key])
            values.append(value)
        valuefinal.append(values)
    valuefinal = sorted(valuefinal, key=lambda x: [x[1]])
    valuefinal.insert(0, title_list)
    return valuefinal  # 返回存储值的数组

def extract_cancel_ticket_values_add_title(listdictionary):
    """cancel_ticket数据类型处理
    Args:
        listdictionary [{},{}......]: _数组+字典类型
    :return:
        valuefinal:list[list] :[] _返回数组类型数据
    """
    if len(listdictionary)==0:
        return []
    title_list = list(listdictionary[0].keys())
    valuefinal = []
    for dictionary in listdictionary:
        values = []  # 创建一个空数组，用于存储字典的值
        # 遍历字典的所有键，并获取对应的值存储到数组中
        for key in dictionary.keys():
            if dictionary[key] == None:
                dictionary[key] = ''
            value = str(dictionary[key])
            values.append(value)
        valuefinal.append(values)
    valuefinal = sorted(valuefinal, key=lambda x: [x[2],x[3]])
    valuefinal.insert(0, title_list)
    return valuefinal  # 返回存储值的数组

def extract_undo_ticket_values_add_title(listdictionary):
    """undo_ticket数据类型处理
    Args:
        listdictionary [{},{}......]: _数组+字典类型
    :return:
        valuefinal:list[list] :[] _返回数组类型数据
    """
    if len(listdictionary)==0:
        return []
    title_list = list(listdictionary[0].keys())
    valuefinal = []
    for dictionary in listdictionary:
        values = []  # 创建一个空数组，用于存储字典的值
        # 遍历字典的所有键，并获取对应的值存储到数组中
        for key in dictionary.keys():
            if dictionary[key] == None:
                dictionary[key] = ''
            value = str(dictionary[key])
            values.append(value)
        valuefinal.append(values)
    valuefinal = sorted(valuefinal, key=lambda x: [x[0],x[1]])
    valuefinal.insert(0, title_list)
    return valuefinal  # 返回存储值的数组

def extract_win_ticket_values_add_title(listdictionary):
    """win_ticket数据类型处理
    Args:
        listdictionary [{},{}......]: _数组+字典类型
    :return:
        valuefinal:list[list] :[] _返回数组类型数据
    """
    if len(listdictionary)==0:
        return []
    title_list = list(listdictionary[0].keys())
    valuefinal = []
    for dictionary in listdictionary:
        values = []  # 创建一个空数组，用于存储字典的值
        # 遍历字典的所有键，并获取对应的值存储到数组中
        for key in dictionary.keys():
            if dictionary[key] == None:
                dictionary[key] = ''
            value = str(dictionary[key])
            values.append(value)
        valuefinal.append(values)
    valuefinal = sorted(valuefinal, key=lambda x: [x[0],x[1],x[2]])
    valuefinal.insert(0, title_list)
    return valuefinal  # 返回存储值的数组

def extract_paid_ticket_values_add_title(listdictionary):
    """paid_ticket数据类型处理
    Args:
        listdictionary [{},{}......]: _数组+字典类型
    :return:
        valuefinal:list[list] :[] _返回数组类型数据
    """
    if len(listdictionary)==0:
        return []
    title_list = list(listdictionary[0].keys())
    valuefinal = []
    for dictionary in listdictionary:
        values = []  # 创建一个空数组，用于存储字典的值
        # 遍历字典的所有键，并获取对应的值存储到数组中
        for key in dictionary.keys():
            if dictionary[key] == None:
                dictionary[key] = ''
            value = str(dictionary[key])
            values.append(value)
        valuefinal.append(values)
    valuefinal = sorted(valuefinal, key=lambda x: [x[0], x[1], x[2]])
    valuefinal.insert(0, title_list)
    return valuefinal  # 返回存储值的数组

def extract_win_ticket_prize_values_add_title(listdictionary):
    """win_ticket_prize数据类型处理
    Args:
        listdictionary [{},{}......]: _数组+字典类型
    :return:
        valuefinal:list[list] :[] _返回数组类型数据
    """
    if len(listdictionary)==0:
        return []
    title_list = list(listdictionary[0].keys())
    valuefinal = []
    for dictionary in listdictionary:
        values = []  # 创建一个空数组，用于存储字典的值
        # 遍历字典的所有键，并获取对应的值存储到数组中
        for key in dictionary.keys():
            if dictionary[key] == None:
                dictionary[key] = ''
            value = str(dictionary[key])
            values.append(value)
        valuefinal.append(values)
    valuefinal = sorted(valuefinal, key=lambda x: [x[0], x[1], x[2]])
    valuefinal.insert(0, title_list)
    return valuefinal  # 返回存储值的数组


def select_result_selection_to_str(select_result, select_desc) :
    """数据类型处理
    Args:
        select_result  {__iter__}: _列表类型
        select_desc  {__iter__}: _列表类型
    :return:
        select_result: {__iter__} :{} _返回列表类型数据
    """
    for sr in select_result:
        for sd in select_desc:
            if sd == 'selection':
                sr[sd] = raw_to_char(sr[sd])
            if sr[sd] == None:
                sr[sd] = ''
            sr[sd] = str(sr[sd])
    return select_result
def getDataFromExcelSheet(sheetname, current_time):
    """数据类型处理
    Args:
        sheetname  str: _表格sheet页名称
        current_time  str: _str字符串类型
    :return:
        listtuple: [] :[] _返回数组类型数据
    """
    listlist = Excel(TEST_DATA_PATH).get_aslist(sheetname, 1, 3)
    listtuple = listlist_to_listtuple(listlist, sheetname, current_time)
    return listtuple

def getAppointDate(tablename, origdata, current_time):
    """数据类型处理
    Args:
        tablename  str: _str表名
        origdata  str: _str测试数据中的时间
        current_time str: _str当前时间
    :return:
        offset_time_str: str :str _返回字符类型数据
    """
    if 'yesterdaytime' in origdata:
        yesterday_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S') + timedelta(days=-1)
        # yesterday_time = datetime.strptime(current_time, '%Y-%m-%d') + timedelta(days=-1)
        yesterday_time_str = yesterday_time.strftime('%Y-%m-%d')
        hms = origdata.split('yesterdaytime')[1]
        if hms == '000':
            return_time = yesterday_time_str + ' 00:00:00'
            return return_time
        else:
            return_time = yesterday_time_str + ' 23:59:59'
            return return_time
    elif 'todaydatetime' in origdata:
        today_time = datetime.strptime(current_time, '%Y-%m-%d  %H:%M:%S')
        today_time_str = today_time.strftime('%Y-%m-%d 00:00:')
        second = origdata.split('todaydatetime')[1]
        today_time_str_return = today_time_str + second
        return today_time_str_return
    if 'thisYear' in origdata:
        current_time_data = datetime.strptime(current_time, '%Y-%m-%d  %H:%M:%S')
        thisYear_time = current_time_data.year
        thisYearMonthDay_time = datetime(thisYear_time, 1, 1)
        YearMonthDay_endtime = thisYearMonthDay_time + timedelta(
            days=365) if thisYearMonthDay_time.year % 4 == 0 else thisYearMonthDay_time + timedelta(days=364)
        hms = origdata.split('thisYear')[1]
        if hms == '0':
            return_time = thisYearMonthDay_time.strftime('%Y-%m-%d 00:00:00')
            return return_time
        else:
            return_time = YearMonthDay_endtime.strftime('%Y-%m-%d 23:59:59')
            return return_time
    else:
        # 处理N秒的
        if tablename in ['ticket', 'paid_ticket', 'win_ticket_prize', 'ticket_page', 'paid_ticket_page', 'win_ticket_prize_page']:
            offset_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=int(origdata))
            offset_time_str = offset_time.strftime('%Y-%m-%d %H:%M:%S')
            return offset_time_str
        # 处理N分钟的
        else:
            offset_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S') + timedelta(minutes=int(origdata))
            offset_time_str = offset_time.strftime('%Y-%m-%d %H:%M:%S')
            return offset_time_str

def getPartionList(sheetname:str) -> list:
    """数据类型处理
    Args:
        sheetname  str: _表格sheet页名称
    :return:
        withpartionlist:  list[int] :[] _返回数组类型数据
    """
    listlist = Excel(TEST_DATA_PATH).get_aslist(sheetname, 1, 3)
    withpartionlist = []
    if sheetname == "ticket" or sheetname == "ticket_page":
        for dataone in listlist:
            withpartionlist.append(int(dataone[0]))
            withpartionlist = set(withpartionlist)
            withpartionlist = list(withpartionlist)
        return withpartionlist
    elif sheetname == "win_ticket" or sheetname == "win_ticket_page" or sheetname == "paid_ticket" or sheetname == "paid_ticket_page":
        for dataone in listlist:
            withpartionlist.append(int(dataone[0]))
            withpartionlist = set(withpartionlist)
            withpartionlist = list(withpartionlist)
        return withpartionlist

def getDataDeleteList(sheetname):
    """数据类型处理
    Args:
        sheetname  str: _表格sheet页名称
    :return:
        datalisttuple:  list[tuple[Any, Any, Any]] : list[tuple[Any, Any, Any] _数组+元组数据类型
    """
    listlist = Excel(TEST_DATA_PATH).get_aslist(sheetname,1,3)
    returnlist = []
    if sheetname in ['cancel_ticket', 'undo_ticket', 'cancel_ticket_page', 'undo_ticket_page']:
        for dataone in listlist:
            returnlist.append(dataone[0])
        return returnlist
    elif sheetname in ['ticket', 'ticket_page']:
        datalisttuple = []
        for dataone in listlist:
            datat = (dataone[0],dataone[1],dataone[2])
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname in ['win_ticket', 'paid_ticket', 'win_ticket_page', 'paid_ticket_page']:
        datalisttuple = []
        for dataone in listlist:
            datat = (dataone[0], dataone[1], dataone[2],dataone[3])
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname in ['win_ticket_prize','win_ticket_prize_page']:
        datalisttuple = []
        for dataone in listlist:
            datat = (dataone[0], dataone[1], dataone[2])
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname in ['game_draw']:
        for dataone in listlist:
            returnlist.append(dataone[0])
        return returnlist


def listlist_to_listtuple(datalist:list, sheetname:str, current_time:str) -> list:
    """数据类型处理
    Args:
        datalist  List[]:_数据数组
        sheetname  str: _表格sheet页名称
        current_time  str: _时间
    :return:
        datalisttuple:   list[tuple] :  list[tuple] _数组+元组数据类型
    """
    datalisttuple = []
    if sheetname == 'ticket':
        for dataone in datalist:
            dataone[4] = getAppointDate('ticket', dataone[4], current_time)
            dataone[6] = char_to_raw(dataone[6])
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'ticket_page':
        for dataone in datalist:
            dataone[4] = getAppointDate('ticket_page', dataone[4], current_time)
            dataone[6] = char_to_raw(dataone[6])
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'cancel_ticket':
        for dataone in datalist:
            dataone[6] = char_to_raw(dataone[6])
            dataone[10] = getAppointDate('cancel_ticket', dataone[10], current_time)
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'cancel_ticket_page':
        for dataone in datalist:
            dataone[6] = char_to_raw(dataone[6])
            dataone[10] = getAppointDate('cancel_ticket_page', dataone[10], current_time)
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'undo_ticket':
        for dataone in datalist:
            dataone[2] = getAppointDate('undo_ticket', dataone[2], current_time)
            dataone[8] = char_to_raw(dataone[8])
            if dataone[12] == 'NULL':
                dataone[12] = None
            if dataone[15] == 'NULL':
                dataone[15] = None
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'undo_ticket_page':
        for dataone in datalist:
            dataone[2] = getAppointDate('undo_ticket_page', dataone[2], current_time)
            dataone[8] = char_to_raw(dataone[8])
            if dataone[12] == 'NULL':
                dataone[12] = None
            if dataone[15] == 'NULL':
                dataone[15] = None
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'win_ticket':
        for dataone in datalist:
            dataone[6] = getAppointDate('win_ticket', dataone[6], current_time)
            dataone[9] = char_to_raw(dataone[9])
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'win_ticket_page':
        for dataone in datalist:
            dataone[6] = getAppointDate('win_ticket_page', dataone[6], current_time)
            dataone[9] = char_to_raw(dataone[9])
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'paid_ticket':
        for dataone in datalist:
            dataone[7] = getAppointDate('paid_ticket', dataone[7], current_time)
            dataone[9] = char_to_raw(dataone[9])
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'paid_ticket_page':
        for dataone in datalist:
            dataone[7] = getAppointDate('paid_ticket_page', dataone[7], current_time)
            dataone[9] = char_to_raw(dataone[9])
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'win_ticket_prize':
        for dataone in datalist:
            dataone[9] = getAppointDate('win_ticket_prize', dataone[9], current_time)
            if dataone[10] == 'NULL':
                dataone[10] = None
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'win_ticket_prize_page':
        for dataone in datalist:
            dataone[9] = getAppointDate('win_ticket_prize_page', dataone[9], current_time)
            if dataone[10] == 'NULL':
                dataone[10] = None
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple
    elif sheetname == 'game_draw':
        for dataone in datalist:
            dataone[1] = getAppointDate('game_draw', dataone[1], current_time)
            dataone[5] = getAppointDate('game_draw', dataone[5], current_time)
            dataone[6] = getAppointDate('game_draw', dataone[6], current_time)
            dataone[12] = getAppointDate('game_draw', dataone[12], current_time)
            dataone[13] = getAppointDate('game_draw', dataone[13], current_time)
            datat = tuple(dataone)
            datalisttuple.append(datat)
        return datalisttuple


def raw_to_char(p_raw: str) -> str:
    """数据类型处理
    Args:
        p_raw  str: _字符串类数据
    :return:
        v_result: str: str _字符串类型
    """
    if p_raw == None:
        return ''
    v_result = ""
    v_str = p_raw + 'w'

    x = v_str.find('s')
    v_result_flag = x
    y = 0

    for i in range(x + 1):
        v_int_origin = ord(v_str[i])
        if v_int_origin <= 100:
            v_result += str(v_int_origin) + '+'
        else:
            v_result += 'x+'

    for i in range(len(v_str)):
        if v_str[i] == 'w':
            y = i
            if v_result_flag > 0:
                v_result += '[' + str(ascii_to_mno(v_str[x + 1:x + 3])) + ']'
                for j in range(3, y - x):
                    v_int_origin = ord(v_str[x + j])
                    if v_int_origin <= 100:
                        v_result += str(v_int_origin) + '+'
                    elif v_int_origin == 120:
                        v_result += '*~'
                    elif v_int_origin == 114:
                        v_result += '#~'
                if v_result.endswith('+'):
                    v_result = v_result[:-1]
                v_result += '~'
            else:
                return "error"
            x = y

    if v_result.endswith('~'):
        v_result = v_result[:-1]

    return v_result


def ascii_to_mno(p_str: str) -> int:
    """返回ascii码
    Args:
        p_str  str: _字符串类数据
    :return:
        (ord(p_str[0]) - 1) * 100 + (ord(p_str[1]) - 1): str: str _字符串类型
    """
    return (ord(p_str[0]) - 1) * 100 + (ord(p_str[1]) - 1)


def char_to_raw(p_processed: str) -> str:
    result = ''
    items = p_processed.split('x')
    matches_parlay = items[0]
    result += chr(int(matches_parlay[0]))
    matches_parlay_items = matches_parlay.split('+')
    for i in range(len(matches_parlay_items) - 2):
        result += chr(int(matches_parlay_items[i + 1]))
    result += chr(115)
    matches = items[1].split('~')
    for i in range(len(matches)):
        match_str = matches[i]
        if match_str[0] == '+':
            match_str = match_str[1:]
        match_items = match_str.split(']')
        match_no = int(match_items[0][1:])
        selection_items = match_items[1].split('+')
        result += chr((match_no // 100) + 1)
        result += chr((match_no % 100) + 1)
        for selection in selection_items:
            result += chr(int(selection))
        result += chr(119)
    result = result[:-1]
    return result

