/* Drop the view showing _instance_ network interfaces, since we cnanot alter
 * those at this time.
 */
DROP VIEW IF EXISTS omicron.public.instance_network_interface;
