Inspired by Snowflake/Greenplum, we believe that there is still a vast market for fully cloud-native databases while being compatible with traditional database syntax. In this context, we have designed a new generation of cloud-native database called **Hieroglyph Data Cloud (Hieros)**.

**Hieros** is a production-grade database compatible with PostgreSQL syntax and suitable for cloud-native data warehousing. Our initial design goal was to leverage the power of the PostgreSQL community to achieve full compatibility, thereby reducing the cost for users to migrate to Hieros.

As the fundamental capabilities continue to improve, the boundaries between different types of databases are becoming increasingly blurred. The boundaries between AP databases, TP databases, data warehouses, data lakes, etc., can become fuzzy due to different series of products such as NewSQL/HTAP/Lakehouse. However, the blurring of boundaries does not imply overlapping or substitutable requirements among them. The contradiction between hardware characteristics and different demands prevents the formation of a unified solution. The blurring of boundaries is achieved by applying new technologies to integrate old requirements or uncover new ones. Different types of specialized databases or more integrated data will still have their own advantages.

**Hieros** is positioned as a cloud-native data warehouse product, compatible with SQL standards, and enhances the efficiency of executing AP scenarios through vectorization and other technologies. In commercial AP scenarios, SQL standards remain an important requirement. At the same time, we provide stream processing and data lake capabilities through microservices and public service components technologies.

**Hieros** solves several challenges in traditional databases, including transaction processing, metadata storage, file management, security encryption, time travel, data sharing, zero-copy replication, and multi-cloud. We will combine the compute engine of PostgreSQL/Greenplum, statelessness, and introduce vectorization technology to build a powerful data warehouse.

Data warehousing is just the starting point on our journey, and we hope to continue making progress in the big data industry. We plan to open the door to data through data lakes and data warehouses, and in the next step, build a data platform by providing unique cloud-based features and multi-engines. Finally, we also plan to enhance our data platform through a data marketplace.

**Core Capabilities**

- Elastic computing: separation of compute and storage, real-time elastic computing capability.
- SQL standards: comprehensive support for SQL standards, including full transaction capabilities.
- Data governance: Time Travel, Data Sharing.
- Security management: role-based access control and encryption capabilities.

**SaaS Service**

- No hardware (virtual or physical) to select, install, configure, or manage.
- Almost no software installation, configuration, or management required.
- Continuous maintenance, management, upgrades, and tuning are handled by Hieros.
- Data and computation can be stored privately.

**Feature List**

- Security, Governance, and Data Protection
- Standard and Extended SQL Support
- Tools and Interfaces
- Connectivity
- Data Import and Export
- Data Sharing
- Database Replication and Failover
- Time Travel and Fail-safe
- Zero-copy Cloning
- Continuous Data Pipelines (stream processing)
  
**DEV**
  
Hieros is a database that has been primarily rewritten using Golang and C++. It utilizes Golang to implement transaction management, file management, MVCC (Multi-Version Concurrency Control), locking, Libpg protocol, and stream processing. The ORCA optimizer is employed for optimization, while the PostgreSQL/Greenplum executor has been customized for further development. These proactive changes have enabled us to achieve capabilities similar to those of Snowflake.0
