
_Pragma("once")
extern "C" {
#include "c.h"
#include "postgres.h"
#include "catalog/indexing.h"
#include "catalog/pg_index.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_db_role_setting.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_description.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_partitioned_table.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_rel.h"
#include "catalog/pg_range.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_seclabel.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_shdepend.h"
#include "catalog/pg_shdescription.h"
#include "catalog/pg_shseclabel.h"
#include "catalog/pg_replication_origin.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_statistic_ext_data.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_config_map.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_resgroup.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/pg_class.h"
#include "catalog/pg_appendonly.h"
#include "utils/syscache.h"
}

#include "cstdint"

struct CatCacheDesc {
	uint64_t reloid;	 /* OID of the relation being cached */
	uint64_t indoid;	 /* OID of index relation for this cache */
	int	nkeys;	 /* # of keys needed for cache lookup */
	int	key[4];	 /* attribute numbers of key attrs */
};


constexpr CatCacheDesc sdb_cacheinfo[] = {
	{AggregateRelationId,		/* AGGFNOID */
		AggregateFnoidIndexId,
		1,
		{
			Anum_pg_aggregate_aggfnoid,
			0,
			0,
			0
		}
	},
	{AccessMethodRelationId,	/* AMNAME */
		AmNameIndexId,
		1,
		{
			Anum_pg_am_amname,
			0,
			0,
			0
		}
	},
	{AccessMethodRelationId,	/* AMOID */
		AmOidIndexId,
		1,
		{
			Anum_pg_am_oid,
			0,
			0,
			0
		}
	},
	{AccessMethodOperatorRelationId,	/* AMOPOPID */
		AccessMethodOperatorIndexId,
		3,
		{
			Anum_pg_amop_amopopr,
			Anum_pg_amop_amoppurpose,
			Anum_pg_amop_amopfamily,
			0
		}
	},
	{AccessMethodOperatorRelationId,	/* AMOPSTRATEGY */
		AccessMethodStrategyIndexId,
		4,
		{
			Anum_pg_amop_amopfamily,
			Anum_pg_amop_amoplefttype,
			Anum_pg_amop_amoprighttype,
			Anum_pg_amop_amopstrategy
		}
	},
	{AccessMethodProcedureRelationId,	/* AMPROCNUM */
		AccessMethodProcedureIndexId,
		4,
		{
			Anum_pg_amproc_amprocfamily,
			Anum_pg_amproc_amproclefttype,
			Anum_pg_amproc_amprocrighttype,
			Anum_pg_amproc_amprocnum
		}
	},
	{AttributeRelationId,		/* ATTNAME */
		AttributeRelidNameIndexId,
		2,
		{
			Anum_pg_attribute_attrelid,
			Anum_pg_attribute_attname,
			0,
			0
		}
	},
	{AttributeRelationId,		/* ATTNUM */
		AttributeRelidNumIndexId,
		2,
		{
			Anum_pg_attribute_attrelid,
			Anum_pg_attribute_attnum,
			0,
			0
		}
	},
	{AuthMemRelationId,			/* AUTHMEMMEMROLE */
		AuthMemMemRoleIndexId,
		2,
		{
			Anum_pg_auth_members_member,
			Anum_pg_auth_members_roleid,
			0,
			0
		}
	},
	{AuthMemRelationId,			/* AUTHMEMROLEMEM */
		AuthMemRoleMemIndexId,
		2,
		{
			Anum_pg_auth_members_roleid,
			Anum_pg_auth_members_member,
			0,
			0
		}
	},
	{AuthIdRelationId,			/* AUTHNAME */
		AuthIdRolnameIndexId,
		1,
		{
			Anum_pg_authid_rolname,
			0,
			0,
			0
		}
	},
	{AuthIdRelationId,			/* AUTHOID */
		AuthIdOidIndexId,
		1,
		{
			Anum_pg_authid_oid,
			0,
			0,
			0
		}
	},
	{CastRelationId,			/* CASTSOURCETARGET */
		CastSourceTargetIndexId,
		2,
		{
			Anum_pg_cast_castsource,
			Anum_pg_cast_casttarget,
			0,
			0
		}
	},
	{OperatorClassRelationId,	/* CLAAMNAMENSP */
		OpclassAmNameNspIndexId,
		3,
		{
			Anum_pg_opclass_opcmethod,
			Anum_pg_opclass_opcname,
			Anum_pg_opclass_opcnamespace,
			0
		}
	},
	{OperatorClassRelationId,	/* CLAOID */
		OpclassOidIndexId,
		1,
		{
			Anum_pg_opclass_oid,
			0,
			0,
			0
		}
	},
	{CollationRelationId,		/* COLLNAMEENCNSP */
		CollationNameEncNspIndexId,
		3,
		{
			Anum_pg_collation_collname,
			Anum_pg_collation_collencoding,
			Anum_pg_collation_collnamespace,
			0
		}
	},
	{CollationRelationId,		/* COLLOID */
		CollationOidIndexId,
		1,
		{
			Anum_pg_collation_oid,
			0,
			0,
			0
		}
	},
	{ConversionRelationId,		/* CONDEFAULT */
		ConversionDefaultIndexId,
		4,
		{
			Anum_pg_conversion_connamespace,
			Anum_pg_conversion_conforencoding,
			Anum_pg_conversion_contoencoding,
			Anum_pg_conversion_oid
		}
	},
	{ConversionRelationId,		/* CONNAMENSP */
		ConversionNameNspIndexId,
		2,
		{
			Anum_pg_conversion_conname,
			Anum_pg_conversion_connamespace,
			0,
			0
		}
	},
	{ConstraintRelationId,		/* CONSTROID */
		ConstraintOidIndexId,
		1,
		{
			Anum_pg_constraint_oid,
			0,
			0,
			0
		}
	},
	{ConversionRelationId,		/* CONVOID */
		ConversionOidIndexId,
		1,
		{
			Anum_pg_conversion_oid,
			0,
			0,
			0
		}
	},
	{DatabaseRelationId,		/* DATABASEOID */
		DatabaseOidIndexId,
		1,
		{
			Anum_pg_database_oid,
			0,
			0,
			0
		}
	},
	{DefaultAclRelationId,		/* DEFACLROLENSPOBJ */
		DefaultAclRoleNspObjIndexId,
		3,
		{
			Anum_pg_default_acl_defaclrole,
			Anum_pg_default_acl_defaclnamespace,
			Anum_pg_default_acl_defaclobjtype,
			0
		}
	},
	{EnumRelationId,			/* ENUMOID */
		EnumOidIndexId,
		1,
		{
			Anum_pg_enum_oid,
			0,
			0,
			0
		}
	},
	{EnumRelationId,			/* ENUMTYPOIDNAME */
		EnumTypIdLabelIndexId,
		2,
		{
			Anum_pg_enum_enumtypid,
			Anum_pg_enum_enumlabel,
			0,
			0
		}
	},
	{EventTriggerRelationId,	/* EVENTTRIGGERNAME */
		EventTriggerNameIndexId,
		1,
		{
			Anum_pg_event_trigger_evtname,
			0,
			0,
			0
		}
	},
	{EventTriggerRelationId,	/* EVENTTRIGGEROID */
		EventTriggerOidIndexId,
		1,
		{
			Anum_pg_event_trigger_oid,
			0,
			0,
			0
		}
	},
	{ExtprotocolRelationId,		/* EXTPROTOCOLOID */
		ExtprotocolOidIndexId,
		1,
		{
			Anum_pg_extprotocol_oid,
			0,
			0,
			0
		}
	},
	{ExtprotocolRelationId,		/* EXTPROTOCOLNAME */
		ExtprotocolPtcnameIndexId,
		1,
		{
			Anum_pg_extprotocol_ptcname,
			0,
			0,
			0
		}
	},
	{ForeignDataWrapperRelationId,	/* FOREIGNDATAWRAPPERNAME */
		ForeignDataWrapperNameIndexId,
		1,
		{
			Anum_pg_foreign_data_wrapper_fdwname,
			0,
			0,
			0
		}
	},
	{ForeignDataWrapperRelationId,	/* FOREIGNDATAWRAPPEROID */
		ForeignDataWrapperOidIndexId,
		1,
		{
			Anum_pg_foreign_data_wrapper_oid,
			0,
			0,
			0
		}
	},
	{ForeignServerRelationId,	/* FOREIGNSERVERNAME */
		ForeignServerNameIndexId,
		1,
		{
			Anum_pg_foreign_server_srvname,
			0,
			0,
			0
		}
	},
	{ForeignServerRelationId,	/* FOREIGNSERVEROID */
		ForeignServerOidIndexId,
		1,
		{
			Anum_pg_foreign_server_oid,
			0,
			0,
			0
		}
	},
	{ForeignTableRelationId,	/* FOREIGNTABLEREL */
		ForeignTableRelidIndexId,
		1,
		{
			Anum_pg_foreign_table_ftrelid,
			0,
			0,
			0
		}
	},
	{GpPolicyRelationId,	/* GPPOLICYID */
		GpPolicyLocalOidIndexId,
		1,
		{
			Anum_gp_distribution_policy_localoid,
			0,
			0,
			0
		}
	},
	{IndexRelationId,			/* INDEXRELID */
		IndexRelidIndexId,
		1,
		{
			Anum_pg_index_indexrelid,
			0,
			0,
			0
		}
	},
	{LanguageRelationId,		/* LANGNAME */
		LanguageNameIndexId,
		1,
		{
			Anum_pg_language_lanname,
			0,
			0,
			0
		}
	},
	{LanguageRelationId,		/* LANGOID */
		LanguageOidIndexId,
		1,
		{
			Anum_pg_language_oid,
			0,
			0,
			0
		}
	},
	{NamespaceRelationId,		/* NAMESPACENAME */
		NamespaceNameIndexId,
		1,
		{
			Anum_pg_namespace_nspname,
			0,
			0,
			0
		}
	},
	{NamespaceRelationId,		/* NAMESPACEOID */
		NamespaceOidIndexId,
		1,
		{
			Anum_pg_namespace_oid,
			0,
			0,
			0
		}
	},
	{OperatorRelationId,		/* OPERNAMENSP */
		OperatorNameNspIndexId,
		4,
		{
			Anum_pg_operator_oprname,
			Anum_pg_operator_oprleft,
			Anum_pg_operator_oprright,
			Anum_pg_operator_oprnamespace
		}
	},
	{OperatorRelationId,		/* OPEROID */
		OperatorOidIndexId,
		1,
		{
			Anum_pg_operator_oid,
			0,
			0,
			0
		}
	},
	{OperatorFamilyRelationId,	/* OPFAMILYAMNAMENSP */
		OpfamilyAmNameNspIndexId,
		3,
		{
			Anum_pg_opfamily_opfmethod,
			Anum_pg_opfamily_opfname,
			Anum_pg_opfamily_opfnamespace,
			0
		}
	},
	{OperatorFamilyRelationId,	/* OPFAMILYOID */
		OpfamilyOidIndexId,
		1,
		{
			Anum_pg_opfamily_oid,
			0,
			0,
			0
		}
	},
	{PartitionedRelationId,		/* PARTRELID */
		PartitionedRelidIndexId,
		1,
		{
			Anum_pg_partitioned_table_partrelid,
			0,
			0,
			0
		}
	},
	{ProcedureRelationId,		/* PROCNAMEARGSNSP */
		ProcedureNameArgsNspIndexId,
		3,
		{
			Anum_pg_proc_proname,
			Anum_pg_proc_proargtypes,
			Anum_pg_proc_pronamespace,
			0
		}
	},
	{ProcedureRelationId,		/* PROCOID */
		ProcedureOidIndexId,
		1,
		{
			Anum_pg_proc_oid,
			0,
			0,
			0
		}
	},
	{PublicationRelationId,		/* PUBLICATIONNAME */
		PublicationNameIndexId,
		1,
		{
			Anum_pg_publication_pubname,
			0,
			0,
			0
		}
	},
	{PublicationRelationId,		/* PUBLICATIONOID */
		PublicationObjectIndexId,
		1,
		{
			Anum_pg_publication_oid,
			0,
			0,
			0
		}
	},
	{PublicationRelRelationId,	/* PUBLICATIONREL */
		PublicationRelObjectIndexId,
		1,
		{
			Anum_pg_publication_rel_oid,
			0,
			0,
			0
		}
	},
	{PublicationRelRelationId,	/* PUBLICATIONRELMAP */
		PublicationRelPrrelidPrpubidIndexId,
		2,
		{
			Anum_pg_publication_rel_prrelid,
			Anum_pg_publication_rel_prpubid,
			0,
			0
		}
	},
	{RangeRelationId,			/* RANGETYPE */
		RangeTypidIndexId,
		1,
		{
			Anum_pg_range_rngtypid,
			0,
			0,
			0
		}
	},
	{RelationRelationId,		/* RELNAMENSP */
		ClassNameNspIndexId,
		2,
		{
			Anum_pg_class_relname,
			Anum_pg_class_relnamespace,
			0,
			0
		}
	},
	{RelationRelationId,		/* RELOID */
		ClassOidIndexId,
		1,
		{
			Anum_pg_class_oid,
			0,
			0,
			0
		}
	},
	{ReplicationOriginRelationId,	/* REPLORIGIDENT */
		ReplicationOriginIdentIndex,
		1,
		{
			Anum_pg_replication_origin_roident,
			0,
			0,
			0
		}
	},
	{ReplicationOriginRelationId,	/* REPLORIGNAME */
		ReplicationOriginNameIndex,
		1,
		{
			Anum_pg_replication_origin_roname,
			0,
			0,
			0
		}
	},
	{ResGroupRelationId,		/* RESGROUPOID */
		ResGroupOidIndexId,
		1,
		{
			Anum_pg_resgroup_oid,
			0,
			0,
			0
		}
	},
	{ResGroupRelationId,		/* RESGROUPNAME */
		ResGroupRsgnameIndexId,
		1,
		{
			Anum_pg_resgroup_rsgname,
			0,
			0,
			0
		}
	},
	{RewriteRelationId,			/* RULERELNAME */
		RewriteRelRulenameIndexId,
		2,
		{
			Anum_pg_rewrite_ev_class,
			Anum_pg_rewrite_rulename,
			0,
			0
		}
	},
	{SequenceRelationId,		/* SEQRELID */
		SequenceRelidIndexId,
		1,
		{
			Anum_pg_sequence_seqrelid,
			0,
			0,
			0
		}
	},
	{StatisticExtDataRelationId,	/* STATEXTDATASTXOID */
		StatisticExtDataStxoidIndexId,
		1,
		{
			Anum_pg_statistic_ext_data_stxoid,
			0,
			0,
			0
		}
	},
	{StatisticExtRelationId,	/* STATEXTNAMENSP */
		StatisticExtNameIndexId,
		2,
		{
			Anum_pg_statistic_ext_stxname,
			Anum_pg_statistic_ext_stxnamespace,
			0,
			0
		}
	},
	{StatisticExtRelationId,	/* STATEXTOID */
		StatisticExtOidIndexId,
		1,
		{
			Anum_pg_statistic_ext_oid,
			0,
			0,
			0
		}
	},
	{StatisticRelationId,		/* STATRELATTINH */
		StatisticRelidAttnumInhIndexId,
		3,
		{
			Anum_pg_statistic_starelid,
			Anum_pg_statistic_staattnum,
			Anum_pg_statistic_stainherit,
			0
		}
	},
	{SubscriptionRelationId,	/* SUBSCRIPTIONNAME */
		SubscriptionNameIndexId,
		2,
		{
			Anum_pg_subscription_subdbid,
			Anum_pg_subscription_subname,
			0,
			0
		}
	},
	{SubscriptionRelationId,	/* SUBSCRIPTIONOID */
		SubscriptionObjectIndexId,
		1,
		{
			Anum_pg_subscription_oid,
			0,
			0,
			0
		}
	},
	{SubscriptionRelRelationId, /* SUBSCRIPTIONRELMAP */
		SubscriptionRelSrrelidSrsubidIndexId,
		2,
		{
			Anum_pg_subscription_rel_srrelid,
			Anum_pg_subscription_rel_srsubid,
			0,
			0
		}
	},
	{TableSpaceRelationId,		/* TABLESPACEOID */
		TablespaceOidIndexId,
		1,
		{
			Anum_pg_tablespace_oid,
			0,
			0,
			0,
		}
	},
	{TransformRelationId,		/* TRFOID */
		TransformOidIndexId,
		1,
		{
			Anum_pg_transform_oid,
			0,
			0,
			0,
		}
	},
	{TransformRelationId,		/* TRFTYPELANG */
		TransformTypeLangIndexId,
		2,
		{
			Anum_pg_transform_trftype,
			Anum_pg_transform_trflang,
			0,
			0,
		}
	},
	{TSConfigMapRelationId,		/* TSCONFIGMAP */
		TSConfigMapIndexId,
		3,
		{
			Anum_pg_ts_config_map_mapcfg,
			Anum_pg_ts_config_map_maptokentype,
			Anum_pg_ts_config_map_mapseqno,
			0
		}
	},
	{TSConfigRelationId,		/* TSCONFIGNAMENSP */
		TSConfigNameNspIndexId,
		2,
		{
			Anum_pg_ts_config_cfgname,
			Anum_pg_ts_config_cfgnamespace,
			0,
			0
		}
	},
	{TSConfigRelationId,		/* TSCONFIGOID */
		TSConfigOidIndexId,
		1,
		{
			Anum_pg_ts_config_oid,
			0,
			0,
			0
		}
	},
	{TSDictionaryRelationId,	/* TSDICTNAMENSP */
		TSDictionaryNameNspIndexId,
		2,
		{
			Anum_pg_ts_dict_dictname,
			Anum_pg_ts_dict_dictnamespace,
			0,
			0
		}
	},
	{TSDictionaryRelationId,	/* TSDICTOID */
		TSDictionaryOidIndexId,
		1,
		{
			Anum_pg_ts_dict_oid,
			0,
			0,
			0
		}
	},
	{TSParserRelationId,		/* TSPARSERNAMENSP */
		TSParserNameNspIndexId,
		2,
		{
			Anum_pg_ts_parser_prsname,
			Anum_pg_ts_parser_prsnamespace,
			0,
			0
		}
	},
	{TSParserRelationId,		/* TSPARSEROID */
		TSParserOidIndexId,
		1,
		{
			Anum_pg_ts_parser_oid,
			0,
			0,
			0
		}
	},
	{TSTemplateRelationId,		/* TSTEMPLATENAMENSP */
		TSTemplateNameNspIndexId,
		2,
		{
			Anum_pg_ts_template_tmplname,
			Anum_pg_ts_template_tmplnamespace,
			0,
			0
		}
	},
	{TSTemplateRelationId,		/* TSTEMPLATEOID */
		TSTemplateOidIndexId,
		1,
		{
			Anum_pg_ts_template_oid,
			0,
			0,
			0
		}
	},
	{TypeRelationId,			/* TYPENAMENSP */
		TypeNameNspIndexId,
		2,
		{
			Anum_pg_type_typname,
			Anum_pg_type_typnamespace,
			0,
			0
		}
	},
	{TypeRelationId,			/* TYPEOID */
		TypeOidIndexId,
		1,
		{
			Anum_pg_type_oid,
			0,
			0,
			0
		}
	},
	{UserMappingRelationId,		/* USERMAPPINGOID */
		UserMappingOidIndexId,
		1,
		{
			Anum_pg_user_mapping_oid,
			0,
			0,
			0
		}
	},
	{UserMappingRelationId,		/* USERMAPPINGUSERSERVER */
		UserMappingUserServerIndexId,
		2,
		{
			Anum_pg_user_mapping_umuser,
			Anum_pg_user_mapping_umserver,
			0,
			0
		}
	},
	{AppendOnlyRelationId,		/* APPENDONLYOID*/
		AppendOnlyRelidIndexId,
		1,
		{
			Anum_pg_appendonly_relid,
			0,
			0,
			0
		}
	}
};