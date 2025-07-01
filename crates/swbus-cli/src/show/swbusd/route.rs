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
    nh_scope: String,
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

    fn process_response(&self, response: &RequestResponse) {
        let routes = match &response.response_body {
            Some(request_response::ResponseBody::RouteQueryResult(route_result)) => route_result,
            _ => {
                info!("Expecting RouteQueryResult but got something else: {:?}", response);
                return;
            }
        };

        let routes: Vec<RouteDisplay> = routes
            .entries
            .iter()
            .map(|entry| RouteDisplay {
                service_path: entry
                    .service_path
                    .as_ref()
                    .expect("service_path in RouteQueryResult cannot be None")
                    .to_longest_path(),
                hop_count: entry.hop_count,
                nh_id: entry.nh_id.clone(),
                nh_scope: RouteScope::try_from(entry.nh_scope).unwrap().as_str_name().to_string(),
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
