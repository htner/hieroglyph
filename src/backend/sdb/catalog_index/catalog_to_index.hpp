#pragma once
#include <cstdint>
#include <map>
#include <vector>
#include <set>


namespace sdb {
static std::map<uint32_t, std::vector<uint32_t>> catalog_to_index = {
	{826, {827, 828}}, // pg_default_acl <rel oid, index oid>
	{1213, {2697, 2698}}, // pg_tablespace <rel oid, index oid>
	{1214, {1232, 1233}}, // pg_shdepend <rel oid, index oid>
	{1247, {2703, 2704}}, // pg_type <rel oid, index oid>
	{1249, {2658, 2659}}, // pg_attribute <rel oid, index oid>
	{1255, {2690, 2691}}, // pg_proc <rel oid, index oid>
	{1259, {2662, 2663, 3455}}, // pg_class <rel oid, index oid>
	{1260, {2676, 2677, 6029, 6440}}, // pg_authid <rel oid, index oid>
	{1261, {2694, 2695}}, // pg_auth_members <rel oid, index oid>
	{1262, {2671, 2672}}, // pg_database <rel oid, index oid>
	{1417, {113, 549}}, // pg_foreign_server <rel oid, index oid>
	{1418, {174, 175}}, // pg_user_mapping <rel oid, index oid>
	{2224, {5002}}, // pg_sequence <rel oid, index oid>
	{2328, {112, 548}}, // pg_foreign_data_wrapper <rel oid, index oid>
	{2396, {2397}}, // pg_shdescription <rel oid, index oid>
	{2600, {2650}}, // pg_aggregate <rel oid, index oid>
	{2601, {2651, 2652}}, // pg_am <rel oid, index oid>
	{2602, {2653, 2654, 2756}}, // pg_amop <rel oid, index oid>
	{2603, {2655, 2757}}, // pg_amproc <rel oid, index oid>
	{2604, {2656, 2657}}, // pg_attrdef <rel oid, index oid>
	{2605, {2660, 2661}}, // pg_cast <rel oid, index oid>
	{2606, {2579, 2664, 2665, 2666, 2667}}, // pg_constraint <rel oid, index oid>
	{2607, {2668, 2669, 2670}}, // pg_conversion <rel oid, index oid>
	{2608, {2673, 2674}}, // pg_depend <rel oid, index oid>
	{2609, {2675}}, // pg_description <rel oid, index oid>
	{2610, {2678, 2679}}, // pg_index <rel oid, index oid>
	{2611, {2187, 2680}}, // pg_inherits <rel oid, index oid>
	{2612, {2681, 2682}}, // pg_language <rel oid, index oid>
	{2613, {2683}}, // pg_largeobject <rel oid, index oid>
	{2615, {2684, 2685}}, // pg_namespace <rel oid, index oid>
	{2616, {2686, 2687}}, // pg_opclass <rel oid, index oid>
	{2617, {2688, 2689}}, // pg_operator <rel oid, index oid>
	{2618, {2692, 2693}}, // pg_rewrite <rel oid, index oid>
	{2619, {2696}}, // pg_statistic <rel oid, index oid>
	{2620, {2699, 2701, 2702}}, // pg_trigger <rel oid, index oid>
	{2753, {2754, 2755}}, // pg_opfamily <rel oid, index oid>
	{2964, {2965}}, // pg_db_role_setting <rel oid, index oid>
	{2995, {2996}}, // pg_largeobject_metadata <rel oid, index oid>
	{3079, {3080, 3081}}, // pg_extension <rel oid, index oid>
	{3118, {3119}}, // pg_foreign_table <rel oid, index oid>
	{3256, {3257, 3258}}, // pg_policy <rel oid, index oid>
	{3350, {3351}}, // pg_partitioned_table <rel oid, index oid>
	{3381, {3379, 3380, 3997}}, // pg_statistic_ext <rel oid, index oid>
	{3394, {3395}}, // pg_init_privs <rel oid, index oid>
	{3429, {3433}}, // pg_statistic_ext_data <rel oid, index oid>
	{3456, {3085, 3164}}, // pg_collation <rel oid, index oid>
	{3466, {3467, 3468}}, // pg_event_trigger <rel oid, index oid>
	{3501, {3502, 3503, 3534}}, // pg_enum <rel oid, index oid>
	{3541, {3542}}, // pg_range <rel oid, index oid>
	{3576, {3574, 3575}}, // pg_transform <rel oid, index oid>
	{3592, {3593}}, // pg_shseclabel <rel oid, index oid>
	{3596, {3597}}, // pg_seclabel <rel oid, index oid>
	{3600, {3604, 3605}}, // pg_ts_dict <rel oid, index oid>
	{3601, {3606, 3607}}, // pg_ts_parser <rel oid, index oid>
	{3602, {3608, 3712}}, // pg_ts_config <rel oid, index oid>
	{3603, {3609}}, // pg_ts_config_map <rel oid, index oid>
	{3764, {3766, 3767}}, // pg_ts_template <rel oid, index oid>
	{5022, {5023}}, // gp_partition_template <rel oid, index oid>
	{5036, {7139, 7140}}, // gp_segment_configuration <rel oid, index oid>
	{5043, {6067}}, // gp_fastsequence <rel oid, index oid>
	{6000, {6001, 6002}}, // pg_replication_origin <rel oid, index oid>
	{6026, {6027, 6028}}, // pg_resqueue <rel oid, index oid>
	{6052, {6054}}, // pg_stat_last_operation <rel oid, index oid>
	{6056, {6057, 6058}}, // pg_stat_last_shoperation <rel oid, index oid>
	{6059, {6061, 6062, 6063}}, // pg_resourcetype <rel oid, index oid>
	{6060, {6442, 6443}}, // pg_resqueuecapability <rel oid, index oid>
	{6070, {6449}}, // pg_auth_time_constraint <rel oid, index oid>
	{6100, {6114, 6115}}, // pg_subscription <rel oid, index oid>
	{6102, {6117}}, // pg_subscription_rel <rel oid, index oid>
	{6105, {7141}}, // pg_appendonly <rel oid, index oid>
	{6106, {6112, 6113}}, // pg_publication_rel <rel oid, index oid>
	{6220, {6207}}, // pg_type_encoding <rel oid, index oid>
	{6231, {6236, 6237, 6238}}, // pg_attribute_encoding <rel oid, index oid>
	{6436, {6444, 6447}}, // pg_resgroup <rel oid, index oid>
	{6439, {6445, 6446}}, // pg_resgroupcapability <rel oid, index oid>
	{7056, {7059}}, // pg_compression <rel oid, index oid>
	{7142, {6103}}, // gp_distribution_policy <rel oid, index oid>
	{7175, {7156, 7177}}, // pg_extprotocol <rel oid, index oid>
	{7176, {9926}}, // pg_proc_callback <rel oid, index oid>
	{7902, {1137}}, // pg_pltemplate <rel oid, index oid>
	{9107, {6110, 6111}} // pg_publication <rel oid, index oid>
};

static std::set<uint32_t> reload_catalog_list = {
  1247, // "pg_type",
  2608, // "pg_depend",
  1259, // "pg_class",
  1249, // "pg_attribute",
  1214, // "pg_shdepend",
  7142, // "gp_distribution_policy",
  6052, // "pg_stat_last_operation",
};
}  // namespace sdb
