<img src="https://user-images.githubusercontent.com/9434884/43697219-3cb4ef3a-9975-11e8-9a9c-73f4f537442d.png" alt="Sentinel Logo" width="50%">

# Sentinel: The Sentinel of Your Microservices

[![Travis Build Status](https://travis-ci.org/alibaba/Sentinel.svg?branch=master)](https://travis-ci.org/alibaba/Sentinel)
[![Codecov](https://codecov.io/gh/alibaba/Sentinel/branch/master/graph/badge.svg)](https://codecov.io/gh/alibaba/Sentinel)
[![Maven Central](https://img.shields.io/maven-central/v/com.alibaba.csp/sentinel-core.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:com.alibaba.csp%20AND%20a:sentinel-core)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Gitter](https://badges.gitter.im/alibaba/Sentinel.svg)](https://gitter.im/alibaba/Sentinel)

## Documentation

See the [中文文档](https://github.com/alibaba/Sentinel/wiki/%E4%BB%8B%E7%BB%8D) for document in Chinese.

See the [Wiki](https://github.com/alibaba/Sentinel/wiki) for full documentation, examples, blog posts, operational details and other information.

Sentinel provides integration modules for various open-source frameworks
(e.g. Spring Cloud, Apache Dubbo, gRPC, Spring WebFlux, Reactor) and service mesh.
You can refer to [the document](https://github.com/alibaba/Sentinel/wiki/Adapters-to-Popular-Framework) for more information.

If you are using Sentinel, please [**leave a comment here**](https://github.com/alibaba/Sentinel/issues/18) to tell us your scenario to make Sentinel better.
It's also encouraged to add the link of your blog post, tutorial, demo or customized components to [**Awesome Sentinel**](./doc/awesome-sentinel.md).

## 源码总结

如下是Sentinel限流设计原理图

![Sentinel限流设计原理](https://gitee.com/onlycreator/draw/raw/master/img/Sentinel限流设计原理.png)

如下是Sentinel源码运行逻辑图，其中涉及到类都有源码注释，因为一次提交了过多的源码，步骤分析我会慢慢给补上。

![Sentinel源码运行原理](https://gitee.com/onlycreator/draw/raw/master/img/Sentinel源码运行原理.png)

