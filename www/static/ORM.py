#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#创建ORM

__author__ = 'peng'

import asyncio, logging
import aiomysql

#创建登录函数
def log(sql, arg=()):
    logging.info('SQL: %s' % sql)

#创建数据连接池
async def creat_pool(loop, **kw):
    logging.info('create database connection pool(创建数据连接池)...')
    global __pool
    __pool = await aiomysql.create_pool(
        #数据库主机名
        host = kw.get('host','localhost'),
        #TCP端口，默认3306
        port = kw.get('port',3306),
        user = kw['user'],
        password = kw['password'],
        #db连接的数据库名
        db = kw['db'],
        charset = kw.get('charset','utf-8'),
        autocommit = kw.get('autocommit',True),
        maxsize = kw.get('maxsize',10),
        minsize = kw.get('minsize',1),
        loop = loop
    )



#选择函数，返回结果集
#需要传入SQL语句和SQL参数,转换成mySQL数据库形式
#如果传入size参数，就通过fetchmany()获取最多指定数量的记录，否则，通过fetchall()获取所有记录
async def select(sql, args, size=None):
    log(sql,args)
    global __pool
    #创建mySQL连接（也就是数据库的登录者）
    async with  __pool.get() as conn:
        #创建游标，使用连接登录数据库
        async with conn.cursor(aiomysql.DictCursor) as cur:
        #操作数据库
        #SQL语句的占位符是?，而MySQL的占位符是%s，select()函数在内部自动替换
        #await将调用一个子协程（也就是在一个协程中调用另一个协程）并直接获得子协程的返回结果
        await cur.excute(sql.replace('?','%s'),args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        #关闭数据库
        logging.info('rows returned(行):%s' % len(rs))
        return rs

#操作函数，不返回结果集，返回结果数
#定义一个通用的操作用于插入，更新和删除函数（Insert, Update, Delete)
async def excute(sql, args, autocommit = True):
    log(sql)
    async with  __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?','%s'),args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise
        return affected

#定义User对象，然后把数据库表users与其关联
class User(Model):
    __table__ = 'users'

    id = IntegerField(primary_key = True)
    name = stringField()

#定义基类Model
class Modle(dict, metaclass = ModelMetaclass):

    def __init__(self, **kw):
        super(Model,self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(r"'Modle' object has no Attribut(对象没有属性：) '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self,key,None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                #获取初始化的值的时候先判断
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key,str(value)))
                setattr(self,key,value)
        return value

#创造替换字符串列表
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

#Field和各种Field子类
Class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        sefl.default = default

    def __str__(self):
        return '<%s, %s: %s>' % (self.__class__.name, self.column_type, self.name)

#映射varchar的StringField，BooleanField,IntegerField,FloatField,TextField
class StringField(Field):

    def __init__(self, name = None,primary_key = False, default = None, ddl = 'varchar(100)'):
        supur().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

#通过metaclass读取子类信息，继承自type，并且通过重写type的__new__方法控制创建的过程
#也就是说metaclass的实例化结果是类，而class实例化的结果是instance
#metaclass是类似创建类的模板，所有的类都是通过它来create的(调用__new__)，这使得你可以自由的控制
#创建类的那个过程，实现你所需要的功能。
class ModelMetaclass(type):

    #定义一个__new__方法，从基类中产生实例
    def __new__(cls, name, bases, attrs):
        #排除Model类本身
        if name == 'Modle':
            return type.__new__(cls, name, bases, attrs)
        #获取table的名称：
        tableName = attrs.get('__table__',None) or name
        logging.info('found model(发现模型): %s (table(列表): %s)' % (name, tableName))
        #获取所有的Filed和主键名
        mappings = dict()
        field = []
        primarykey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('  found mapping(发现映射关系): %s ==> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:
                    # 找到主键:
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field(复制初始key值失败): %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found（未发现初始值）.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

#往Model类添加class方法，就可以让所有子类调用class方法
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value

    #添加findAll方法
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    #添加findNumber方法
    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']
    #初始值查找（find）方法
    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)
