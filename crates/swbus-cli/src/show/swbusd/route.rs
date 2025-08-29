use crate::show::ShowCmdHandler;
use crate::CommandContext;
use clap::Parser;
use swbus_proto::swbus::*;
use tabled::{Table, Tabled};
use tracing::info;

#[derive(Parser, Debug)]
pub struct ShowRouteCmd {}

#[derive(Tabled)]
struct RouteDisplay {
    service_path: String,
    hop_count: u32,
    nh_id: String,
    route_scope: String,
    nh_service_path: String,
}

impl ShowCmdHandler for ShowRouteCmd {
    fn create_request(&self, ctx: &CommandContext, src_sp: &ServicePath) -> SwbusMessage {
        let mgmt_req = ManagementRequest::new(ManagementRequestType::SwbusdGetRoutes);
        let swbusd_sp = ctx.sp.to_swbusd_service_path();
        let header = SwbusMessageHeader::new(src_sp.clone(), swbusd_sp, ctx.id_generator.generate());

        SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::ManagementRequest(mgmt_req)),
        }
    }

    fn process_response(&self, ctx: &CommandContext, response: &RequestResponse) {
        let routes = match &response.response_body {
            Some(request_response::ResponseBody::RouteEntries(route_result)) => route_result,
            _ => {
                info!("Expecting RouteEntries but got something else: {:?}", response);
                return;
            }
        };

        let my_sp = Some(ctx.sp.clone());
        let routes: Vec<RouteDisplay> = routes
            .entries
            .iter()
            // Filter out the entry for the show_route request itself
            .filter(|entry| entry.service_path != my_sp)
            .map(|entry| RouteDisplay {
                service_path: entry
                    .service_path
                    .as_ref()
                    .expect("service_path in RouteQueryResult cannot be None")
                    .to_longest_path(),
                hop_count: entry.hop_count,
                nh_id: entry.nh_id.clone(),
                route_scope: RouteScope::try_from(entry.route_scope).unwrap().as_str_name().to_string(),
                nh_service_path: entry
                    .nh_service_path
                    .as_ref()
                    .expect("nh_service_path in RouteQueryResult cannot be None")
                    .to_longest_path(),
            })
            .collect();
        let table = Table::new(routes);
        info!("{}", table)
    }
}
