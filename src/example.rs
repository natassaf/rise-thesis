// use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
// use serde::{Deserialize, Serialize};

// async fn greet(req: HttpRequest) -> impl Responder {
//     let name = req.match_info().get("name").unwrap_or("World");
//     format!("Hello {}!", &name)
// }

// async fn handle_submit_task()->impl Responder {
//     format!("Hello world!")
// }

// #[derive(Serialize, Deserialize)]
// struct SubmitTaskRequest {
//     task: String,
//     number: u64,
// }

// const MAX_SIZE: usize = 262_144; // max payload size is 256k

// #[post("/")]
// async fn index_manual(mut payload: web::Payload) -> Result<HttpResponse, Error> {
//     let mut body = web::BytesMut::new();
//     while let Some(chunk) = payload.next().await {
//         let chunk = chunk?;
//         // limit max size of in-memory payload
//         if (body.len() + chunk.len()) > MAX_SIZE {
//             return Err(error::ErrorBadRequest("overflow"));
//         }
//         body.extend_from_slice(&chunk);
//     }

//     // body is loaded, now we can deserialize serde-json
//     let obj = serde_json::from_slice::<MyObj>(&body)?;
//     Ok(HttpResponse::Ok().json(obj)) // <- send response
// }

// #[actix_web::main]
// async fn main() -> std::io::Result<()> {
//     println!("Server started");
//     HttpServer::new(|| {
//         let app = App::new().route("/submit_task", web::post().to(handle_submit_task));
//         app
//     }).bind("[::]:8080")?
//     .run().await
// }
