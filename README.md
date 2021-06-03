[English](https://github.com/baidu-doris/incubator-doris/blob/master/README_EN.md)

# Apache Doris 百度镜像库

这里是由 Baidu Doris(Palo) 团队维护的 [Apache Doris](https://github.com/apache/incubator-doris) 的镜像库。

该镜像库仅用于发布基于 Apache Doris 官方 Release 版本的 [3 位迭代版本(tags)](https://semver.org/lang/zh-CN/)。包括快速的 Bug 修复和新功能更新。

这些 3 位迭代版本都在百度内部进行过测试和上线，并且所有代码提交均存在与官方仓库中，推荐使用。

所有 3 位迭代版本请看 [这里](https://github.com/baidu-doris/incubator-doris/tags)

## 特别声明

**Apache、Apache Doris 以及 Doris 名称归属于 Apache 软件基金会。**

**本仓库代码非 Apache 官方 Release 版本，但与 Apache 版本完全兼容。官方版本请前往 [官方网站](doris.apache.org) 下载。**

## 使用说明

目前 Apache Doris 的 [官方Release版本](https://github.com/apache/incubator-doris/tags) 为：

* 0.9.0
* 0.10.0
* 0.11.0
* 0.12.0
* 0.13.0

本仓库主要基于 2 位官方版本发布 3 位迭代版本。如：

* 0.9.22-release
* 0.10.23-release
* 0.11.44-release
* 0.12.21-release
* 0.13.12-release

所有 3 位版本可以安全的从对应的官方 2 位版本升级。3 位版本本身也是兼容的，可以安全升级。举例如下：

* 官方 `0.12.0-rc02` 可以升级至 `0.12.21-release`
* `0.11.10-release` 可以升级至 `0.11.44-release`
* `0.11.44-release` 可以升级至 `0.12.21-release`

建议在升级 2 位版本之前，先升级到对应的最新的 3 位版本后，再升级 2 位版本。举例如下：

1. 当前使用版本为 `0.11.10-release`，想升级到 0.12 版本。
2. 首先升级到 `0.11.44-release`，即 0.11 的最新 3 位版本。
3. 再升级到 `0.12.21-release`，即 0.12 的最新 3 位版本。

3 位迭代版本也可以安全的和官方 2 位版本升级。如以下的升级序列是安全的

1. `0.11.10-release`  Baidu 库
2. `0.11.44-release`  Baidu 库
3. `DORIS-0.12-rc02`  官方库
4. `0.12.21-release`  Baidu 库

## Doris 预编译二进制下载

某些情况下，可能用户无法顺利的通过源代码编译的方式得到 Doris 的二进制文件。这里我们提供对应三位版本的预编译二进制下载

* 我们依然**强烈推荐**用户自行通过源码编译产生二进制文件。
* 这里提供的预编译二进制文件仅在 CentOS 7.3, Intel(R) Xeon(R) Gold 6148 CPU @ 2.40GHz 上执行通过。在其他系统或 CPU 型号下，可能会因为 glibc 版本或者 CPU 支持的指令集不同，而导致程序无法运行。
* 预编译二进制文件的 FE 部分使用 Oracle JDK 1.8 编译，请确保运行时依然使用 Oracle JDK 1.8 版本。
* 预编译包含如下组件
    1. Frontend
    2. Backend
    3. Broker
    4. Frontend plugins jars
    5. Spark-Doris-Connector jars

### 下载链接

[下载连接](http://palo.baidu.com/docs/%E4%B8%8B%E8%BD%BD%E4%B8%93%E5%8C%BA/%E9%A2%84%E7%BC%96%E8%AF%91%E7%89%88%E6%9C%AC%E4%B8%8B%E8%BD%BD)

## 百度数据仓库 Palo

百度数据仓库 Palo 提供基于 Doris 的企业级数据仓库托管服务，新用户可0元免费试用3个月。

**试用、多云支持、私有化部署**等请前往：[https://cloud.baidu.com/product/palo.html](https://cloud.baidu.com/product/palo.html) 详询。

欢迎关注 ApacheDoris 官方微信公众号获取更多使用案例和技术文章。

![](https://github.com/baidu-doris/incubator-doris/blob/master/docs/resources/doris-wechat.jpg)

