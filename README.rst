Dynamodb to DataLake Project
==============================================================================


Overview
------------------------------------------------------------------------------
本项目是一个 Demo 项目, 展示了如何使用 `AWS Glue + Apache Hudi <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-hudi.html>`_ 将数据从 `Amazon Dynamodb <https://aws.amazon.com/dynamodb/>`_ 以近实时 (时延小于 5 分钟) 的方式不断写入 `S3 DataLake <https://aws.amazon.com/big-data/datalakes-and-analytics/datalakes/>`_.

**背景信息**

Amazon DynamoDB 是一款非常流行的 NoSQL 数据库, 有着近乎无限扩容的能力以及超高读写性能. 不过 DynamoDB 并不是为大数据分析而设计的, 直接用它来进行查询分析经常需要做全表扫描, 这样不仅性能低下, 成本也高, 还会影响正常业务的性能. 所以为了能分析 DynamoDB 中的数据, 企业往往会利用 `Export to S3 <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html>`_ 功能定期 (例如每天晚上) 将全量数据导出到 S3, 然后用导出的数据构建一个 Data Warehouse 来进行分析. 虽然该解决方案非常简单, 容易实现, 不过它也存在如下问题:

1. 全量数据导出对于较大的表通常要花费很长时间. 超过 1M 条数据的表导出时间就超过了 10 分钟. 对于 1B 以上的表超过 1 小时也很正常.
2. 我们需要对导出的数据进行简单的入仓处理 (转换格式并存入数据仓库). 和前一步类似, 花费的事件也在 10 分钟 到 1 小时以上不等.
3. 上面两个步骤随着数据量的增加, 每次做的时候耗费的时间和资源也会增加.

可以看出, 该解决方案可以实现离线查询, 但是无法实现近实时查询.

**相关技术简介**

`Apache Hudi <https://hudi.apache.org/>`_ 是新一代的数据湖引擎, 主打的是用价格低廉的云存储来实现高吞吐量, transactional, 近实时的不断变化的数据入湖.

`AWS Glue <https://aws.amazon.com/glue/>`_ 则是一款基于 Spark 的无服务器服务, 能让用户无需管理基础设施就能使用 Spark 大数据引擎. 自 2022-11 月 Glue 发布了 4.0 起, 它增加了对 Hudi 的原生支持, 使得在 AWS Glue 上使用 Hudi 变得无比简单.

`DynamoDB Export to S3 <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html>`_: 是 DynamoDB 的一个原生功能. 它能将根据底层的 write ahead log 日志, 计算出某个时间点的最终全量数据状态, 并导出到 S3. 该功能能够指定将某个时间点的数据全部导出到 S3. 这部分数据也通常被称为 Initial Load, 也就是初始数据.

`DynamoDB Stream <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html>`_ 是 DynamoDB 的一个原生功能, 能将 Change of Data Capture (CDC) 数据以实时的方式推送到一个数据流中, 从而能被各种程序所消费. 例如你可以用一个 Lambda Function 处理数据流中的数据. 这部分数据也通常被称为 Incremental Data, 也就是增量数据.

**关于其他数据库数据入湖**

本项目以 Amazon DynamoDB 为例, 介绍了 Trnasactional 数据库数据的基本策略. 在本质上, 其他的数据库入湖也遵循着类似的思路, 细节上会略有不同. 因为 DynamoDB 这款数据库无需安装, 开箱即用, 也没有前期费用, 用多少花多少钱, 所以非常适合做为 Demo 来快速上手, 帮助用户了解数据库入湖的核心技术.

**我们的目标**

我们希望能以近实时的方式将 DynamoDB 的数据导出到数据湖中, 并能对其进行高性能数据分析. 下一节, 我们将介绍能达成这一目标的解决方案.


The Solution
------------------------------------------------------------------------------
**两个核心步骤**

1. 使用 DynamoDB Export to S3 功能, 将某个时间节点的全量数据导出到 S3, 并将这些数据处理后入湖, 作为 Initial load. 如果你的数据很多, 那么这一步会比较耗时. 但是这一步总共只需要做一次, 以后的增量数据将会用别的策略进行处理.
2. 使用 DynamoDB Stream 功能将增量数据导出到 Stream 中, 然后用 Lambda Function 将其按照数据的更新时间分区后写入到 S3 中. 这些增量数据将会被每分钟运行一次的 AWS Glue + Hudi Job 所处理, 并写入到数据湖中. 由于增量数据的大小一般不会太大, 所以每次对增量数据进行处理的耗时是可预估的, 并且不会太长. 这样就能实现近实时的数据入湖了.

**时延分析**

1. 由于 Initial Load 只会处理一次, 所以它对总体时延没有影响.
2. DynamoDB Stream 中的数据被 Lambda Function 所处理的过程中有一些 buffer 的设置, 例如每当 buffer 中有最多 X 条数据, 或是积攒了 Y 秒的数据, 就要被 Lambda Function 所处理. 加入我们的 Y = 10 秒, 那么这部分的时延就是 10 秒.
3. 我们每隔一段时间就启动 AWS Glue + Hudi Job 处理增量数据, 如果这部分的时延是 1 分钟, 而 Job 的运行时间也是 1 分钟, 那么这部分的总时延就是 2 分钟.

综上所述, 总体的时延可以轻松地被控制到 5 分钟以内, 也就是近实时时延.

**一些限制**

1. 所有的 record 必须要有一个唯一的主键, 这个主键既可以是一个单独的字段, 也可以是多个字段的组合. 这个主键会被用于 Hudi record key.
2. 所有的 record 被创建或者被修改时都必须要有一个时间戳, 这个时间戳会被用于 Hudi precombine key. 也就是说有多个 record 如果他们的主键一样, 那么以时间戳大的那个为准.
3. 你的数据最好有 partition key, 可以将数据分散到不同的 S3 partition 中, 有助于提高查询性能. 通常我们会将 record 被创建的时间作为 partition key, 按照年月日分区.


Solution Walkthrough
------------------------------------------------------------------------------
1. **创建 DynamoDB 表**. 你的业务将不断地将数据写入到这个表中, 并可能会对数据进行更新. 在本例中我们不考虑数据删除的情况, 我们希望将目光聚焦到核心的数据入湖技术上. 在实际项目中, 对数据库中的数据进行删除有软删除和永久删除两种方式, 而在数据湖中也有对应的两种方式, 我们需要根据业务需求决定到底使用哪种排列组合, 从而最终决定我们的具体实现. 所以这里我们不展开讨论.
2. **为该 DynamoDB 表启用 Point-in-time recovery (PITR) 功能**. 这样才会启用 Write ahead log, 你才能使用 Export to S3 功能.
3. **启用 DynamoDB Stream 功能**.
4. **创建 DynamoDB Stream Consumer Lambda Function, 并让这个 Lambda Function Subscribe 这个 Stream**. 前面有一些数据没有被 Stream 所捕获不要紧, 这些没被 Stream 的数据反正会在 Export to S3 的过程中被导出. 只要我们导出之前把 Lambda Function 设置好就可以了. 就算我们的 Lambda Function 实现出现了错误, 我们只需要修复错误, 然后重新进行一次 Export to S3 就可以了. Lambda Function 对 DynamoDB Stream 中的数据消费之后, 将会按照数据被 Update 的时间分区保存在 S3 上. 这样 Glue Job 就可以根据时间分区获得某个时间段之间的增量数据了. 它的 S3 folder structure 如下图所示::

    s3://bucket/prefix/2023-07-01-00-00/1.json
    s3://bucket/prefix/2023-07-01-00-00/2.json
    s3://bucket/prefix/2023-07-01-00-00/...

    s3://bucket/prefix/2023-07-01-00-01/1.json
    s3://bucket/prefix/2023-07-01-00-01/2.json
    s3://bucket/prefix/2023-07-01-00-01/...

    s3://bucket/prefix/2023-07-01-00-02/1.json
    s3://bucket/prefix/2023-07-01-00-02/2.json
    s3://bucket/prefix/2023-07-01-00-02/...

    ...

5. **进行 Export to S3**. 将某个时间点的全量数据导出到 S3. 并记录这个时间点, 我们之后要用这个时间点来决定从哪里开始处理增量数据. 导出的全量数据的 S3 目录结构如下::

    # ${root} 该次导出的数据都放在这个目录下, 其中 timestamp_in_milliseconds 是导出数据的时间戳
    s3://${bucket}/${prefix}/AWSDynamoDB/${timestamp_in_milliseconds}-${8_character_random_hex_string}/

    # inside s3://${bucket}/${prefix}/AWSDynamoDB/${timestamp_in_milliseconds}-${8_character_random_hex_string}/
    ${root}/manifest-files.json # 是一个 manifest 文件, 记录了 data 目录下的文件列表以及每个文件有多少条数据
    ${root}/manifest-files.md5
    ${root}/manifest-summary.json # 记录了这个 export 的一些总览信息, 例如数据到什么时候, 总共导出了多少条数据等等.
    ${root}/manifest-summary.md5
    ${root}/data/ # 导出的数据文件
    ${root}/data/44joo5mwg4zmni62jtl3di4q3q.json.gz
    ${root}/data/d65ybi7orm4zpadpdde6vjgwui.json.gz
    ${root}/data/...

6. **创建并运行一个 Initial Load Glue Job**, 将全量数据写入到 Hudi Table.
7. **创建一个增量数据处理的 Incremental Glue Job**. 它接受 1 个 S3 Uri 作为参数. 这个 S3 Uri 是一个 JSON 文件, 里面保存的是 Incremental 数据文件的列表. 而这个 Glue Job 的业务逻辑则是将指定的 Incremental 数据文件中的数据写入到 Hudi Table 中. 注意, 这里我们只创建这个 Glue Job, 而不运行它. 我们将会有一个专门用来 Orchestrate 的 Lambda Function 来运行这个 Glue Job.
8. **创建一个 Orchestrator Lambda Function**, 它会从 #5 中的时间点开始向后扫描在 #4 步骤中介绍过的 S3 目录结构. 根据文件夹的名字可以很轻易的判断出从上一个时间点开始到最新的数据之间的增量数据文件有哪些, 然后就可以运行 #7 中的 Incremental Glue Job 来将增量数据写入到 Hudi Table 了. 注意, 这个 Lambda Function 的编排逻辑中要有能判断是否有正在运行中的 Incremental Glue Job 的能力, 并且要等待这个 Incremental Glue Job 运行完毕之后才能运行下一个 Incremental Glue Job. 这样才能保证数据的一致性. 每次完成一个 Incremental Glue Job 后要记录目前为止最新数据的时间戳, 以便下一次运行的时候从这个时间点开始扫描.


Runbook
------------------------------------------------------------------------------

.. code-block:: bash

    virtualenv -p python3.8 .venv

    source .venv/bin/activate

    pip install -r requirements.txt


Proof of Concept
------------------------------------------------------------------------------
在这一节里, 我们将会创建一个具体的 DynamoDB 用来处理具体的业务逻辑. 我们还会创建一个 Data Ingest 脚本用来模拟业务不断地将数据写入到 DynamoDB 的过程.


DynamoDB Table Data Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
在本例中, DynamoDB Table 中储存的是银行账户的转账数据. 每一条数据都是一次转账记录. 它包含以下字段:

.. code-block:: python

    class Transaction:
        account: str # 账号名, 例如 123-456-7890
        create_at: str # 该笔 Transaction 被创建的时间戳, 例如 2020-01-01 00:00:00.123456
        update_at: str # 该笔 Transaction 被更新的时间戳, 例如 2020-01-01 00:00:00.123456, 只有 note 可以被更改
        entity: str # 谁跟这个账户发生了转账关系, 可以是另一个账户, 也可以是一个 business name
        amount: int # 转账金额
        is_credit: int  # 0 or 1, 0 表示是 debit, 也就是 account 把钱给别人, 1 表示别人把钱给 account
        note: str # 对于该笔转账的注释, 例如 "转账给了张三", 这个注释可以被更改

其中 account 是 hash key, create_at 是 range key. 同一个账户在同一时间只能有一条转账记录. 但是同一个账户在不同时间可以有多条转账记录. 在我们的业务逻辑里, 数据一旦被创建就不会被删除, 但是可以被更新. 更新时你只能修改 note 字段, 其他字段一旦被创建后都不能被修改.


Initial Load
------------------------------------------------------------------------------
我们在某个时间 `将 DynamoDB 中的数据全部导出到 S3 <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/S3DataExport.HowItWorks.html>`_ 中作为一个 Snapshot. 然后将导出的数据处理后存入一个 Hudi Table 作为数据的初始状态. 从这之后, 我们用 DynamoDB Stream 来捕获增量数据, 然后用 Glue Job 来将增量数据写入到 Hudi Table.

**DynamoDB Export 数据格式**

如果你指定 Export 到 ``s3://${bucket}/${prefix}/``, 那么最终 Export 出来的文件将会被保存到这个目录下 ``s3://${bucket}/${prefix}/AWSDynamoDB/${timestamp_in_milliseconds}-${8_character_random_hex_string}/``, 其中 ``${timestamp_in_milliseconds}`` 是精确到毫秒的时间戳, 而 ``${8_character_random_hex_string}`` 是一个随机的 8 位十六进制字符串. 例如 ``01688184000000-a1b2c3d4``.

下面我们来看一下这个目录里面的文件结构. 为了方便表示, 我们假设这个目录是 ``${root}``. ``${root}`` 目录中的文件结构如下.

- ``${root}/data/``: 导出的数据文件
- ``${root}/manifest-files.json``: 是一个 manifest 文件, 记录了 data 目录下的文件列表以及每个文件有多少条数据
- ``${root}/manifest-files.md5``:
- ``${root}/manifest-summary.json``: 记录了这个 export 的一些总览信息, 例如数据到什么时候, 总共导出了多少条数据等等.
- ``${root}/manifest-summary.md5``:

其中 ``${root}/data/``: 目录的文件结构如下. 里面平铺放着很多 ``.json.gz`` 文件. 每个文件中用 JSON line 格式包含了许多条数据, 并且都是被压缩过的. 每一行代表着一个 DynamoDB item.

- ``${root}/data/44joo5mwg4zmni62jtl3di4q3q.json.gz``
- ``${root}/data/d65ybi7orm4zpadpdde6vjgwui.json.gz``
- ...

下面给出了一个数据文件内容的示例::

    {"Item":{"account":{"S":"651-232-2439"},"create_at":{"S":"2023-07-27T22:14:26.066612+0000"},"entity":{"S":"Brown, Christian and Becker"},"note":{"S":"Three way peace sing town."},"update_at":{"S":"2023-07-27T22:14:26.066612+0000"},"amount":{"N":"592"},"is_credit":{"N":"0"}}}
    {"Item":{"account":{"S":"683-757-7274"},"create_at":{"S":"2023-07-27T22:13:16.235000+0000"},"entity":{"S":"Burke-Anderson"},"note":{"S":"Owner wait never water drop."},"update_at":{"S":"2023-07-27T22:13:16.235000+0000"},"amount":{"N":"938"},"is_credit":{"N":"0"}}}
    {"Item":{"account":{"S":"071-548-6730"},"create_at":{"S":"2023-07-27T22:12:26.594639+0000"},"entity":{"S":"Smith, Key and Sparks"},"note":{"S":"Close someone down next."},"update_at":{"S":"2023-07-27T22:12:26.594639+0000"},"amount":{"N":"763"},"is_credit":{"N":"1"}}}
    ...

**Hudi Table Data Modeling**

.. code-block:: python

    class Transaction:
        account: str
        create_at: str
        create_year: str
        create_month: str
        create_day: str
        create_hour: str
        create_minute: str
        update_at: str
        entity: str
        amount: int
        is_credit: int
        note: str

    hudi_options = {
        "hoodie.datasource.write.recordkey.field": "account,create_at",
        "hoodie.datasource.write.partitionpath.field": "create_year,create_month,create_day,create_hour,create_minute",
        "hoodie.datasource.write.precombine.field": "update_at",
    }

**实际操作**

第一次有 7269 条数据.

DynamoDB Stream Lambda Function Output File Content:

.. code-block:: python

    {"account": "622-331-1164", "create_at": "2023-07-30T16:49:47.237081+0000", "update_at": "2023-07-30T16:49:47.237081+0000", "entity": "May, English and Hartman", "amount": 452, "is_credit": 0, "note": "Specific indeed or opportunity determine trial."}
    {"account": "755-987-0981", "create_at": "2023-07-30T16:49:47.394896+0000", "update_at": "2023-07-30T16:49:47.394896+0000", "entity": "Bowman and Sons", "amount": 865, "is_credit": 1, "note": "Product simply assume."}
    {"account": "045-465-4079", "create_at": "2023-07-30T16:49:47.573820+0000", "update_at": "2023-07-30T16:49:47.573820+0000", "entity": "Palmer, Peters and Johnson", "amount": 738, "is_credit": 1, "note": "Avoid girl situation name view."}
    ...

DynamoDB Export to S3 Output File Content

.. code-block:: python

    {"Item":{"account":{"S":"602-943-1702"},"create_at":{"S":"2023-07-30T16:49:36.444736+0000"},"entity":{"S":"James, Lopez and Welch"},"note":{"S":"Be Mrs small will organization everybody sign."},"update_at":{"S":"2023-07-30T16:49:36.444736+0000"},"amount":{"N":"282"},"is_credit":{"N":"1"}}}
    {"Item":{"account":{"S":"729-692-8634"},"create_at":{"S":"2023-07-30T16:49:19.238896+0000"},"entity":{"S":"Pena, Harrison and Cummings"},"note":{"S":"On common speak cultural day protect."},"update_at":{"S":"2023-07-30T16:49:19.238896+0000"},"amount":{"N":"518"},"is_credit":{"N":"1"}}}
    {"Item":{"account":{"S":"120-161-2287"},"create_at":{"S":"2023-07-30T16:49:20.627894+0000"},"entity":{"S":"Wall-Moreno"},"note":{"S":"Various type past mouth daughter reality husband national."},"update_at":{"S":"2023-07-30T16:49:20.627894+0000"},"amount":{"N":"990"},"is_credit":{"N":"0"}}}
    ...

DynamoDB Export to S3 Processed File Content

.. code-block:: python

    {"id": "account:602-943-1702,create_at:2023-07-30T16:49:36.444736+0000", "account": "602-943-1702", "create_at": "2023-07-30T16:49:36.444736+0000", "create_year": "2023", "create_month": "07", "create_day": "30", "create_hour": "16", "create_minute": "49", "update_at": "2023-07-30T16:49:36.444736+0000", "entity": "James, Lopez and Welch", "amount": 282, "is_credit": 1, "note": "Be Mrs small will organization everybody sign."}
    {"id": "account:729-692-8634,create_at:2023-07-30T16:49:19.238896+0000", "account": "729-692-8634", "create_at": "2023-07-30T16:49:19.238896+0000", "create_year": "2023", "create_month": "07", "create_day": "30", "create_hour": "16", "create_minute": "49", "update_at": "2023-07-30T16:49:19.238896+0000", "entity": "Pena, Harrison and Cummings", "amount": 518, "is_credit": 1, "note": "On common speak cultural day protect."}
    {"id": "account:120-161-2287,create_at:2023-07-30T16:49:20.627894+0000", "account": "120-161-2287", "create_at": "2023-07-30T16:49:20.627894+0000", "create_year": "2023", "create_month": "07", "create_day": "30", "create_hour": "16", "create_minute": "49", "update_at": "2023-07-30T16:49:20.627894+0000", "entity": "Wall-Moreno", "amount": 990, "is_credit": 0, "note": "Various type past mouth daughter reality husband national."}
    ...
