// Constante para la API
const EMR_HOST = "ec2-44-220-168-251.compute-1.amazonaws.com";
const KAFKA_PRODUCER_HOST = "ec2-35-173-137-173.compute-1.amazonaws.com";
const API_BASE_URL = "http://"+EMR_HOST+":5000";

// Función para manejar los clics en las estrellas
let selectedRating = 0;
document.querySelectorAll('.stars span').forEach(star => {
  star.addEventListener('click', function () {
    selectedRating = parseInt(this.getAttribute('data-value'));
    highlightStars(selectedRating);
  });
});

// Resaltar las estrellas seleccionadas
function highlightStars(rating) {
  document.querySelectorAll('.stars span').forEach(star => {
    if (parseInt(star.getAttribute('data-value')) <= rating) {
      star.classList.add('active');
    } else {
      star.classList.remove('active');
    }
  });
}

// Enviar la votación mediante una solicitud POST
function sendVote(ttconst, vote) {
  const url = `http://${KAFKA_PRODUCER_HOST}:8082/topics/movie_vote`;
  const headers = {
    "Content-Type": "application/vnd.kafka.json.v2+json"
  };
  const payload = JSON.stringify({
    records: [{ value: `${ttconst}_${vote}` }]
  });

  fetch(url, {
    method: "POST",
    headers: headers,
    body: payload
  })
    .then(response => {
      if (response.ok) {
        console.log("Se envió correctamente");
      } else {
        console.error("Error al enviar la votación");
      }
    })
    .catch(error => console.error("Error:", error));
}

// Manejar el envío del voto
function submitVote() {
  const movieId = document.getElementById("movieId").value;
  if (!movieId) {
    alert("Por favor, ingresa el ID de la película");
    return;
  }
  if (selectedRating === 0) {
    alert("Por favor, selecciona una puntuación");
    return;
  }
  sendVote(movieId, selectedRating);
}

function refreshRatings() {
  fetch(`${API_BASE_URL}/get_last_votes`)
      .then(response => response.json())
      .then(data => {
          const tbody = document.getElementById("ratingsTable").querySelector("tbody");
          tbody.innerHTML = ""; // Limpiar tabla

          // Ordenar la data por el timestamp (parte después del guion bajo en row_key)
          data.sort((a, b) => {
              const timestampA = a.row_key.split("_")[1]; // Obtener el timestamp de a
              const timestampB = b.row_key.split("_")[1]; // Obtener el timestamp de b
              return timestampB - timestampA; // Ordenar en orden descendente
          });

          // Recorrer los datos ordenados
          data.forEach(entry => {
              const tconst = entry.row_key.split("_")[0]; // Extraer el tconst
              const vote = entry.data["cf:vote"]; // Obtener el voto

              const row = document.createElement("tr");
              row.innerHTML = `
                  <td>${tconst}</td>
                  <td>${vote}</td>
              `;
              tbody.appendChild(row);
          });
      })
      .catch(error => console.error("Error al cargar los datos:", error));
}



// Evento del botón de refresh
document.getElementById("refreshButton").addEventListener("click", refreshRatings);

async function fetchMovieRating() {
  console.log("fetching rating....")
  try {
    const movieId = document.getElementById("movieId").value;
    const response = await fetch(`${API_BASE_URL}/get_rating_by_movie_id?movie_id=${movieId}`);
    if (!response.ok) throw new Error("Error al obtener los datos de la película");
    const data = await response.json();

    // Extraer y formatear datos
    const averageRating = parseFloat(data.averagerating).toFixed(2); // Usamos 'averagerating' del JSON
    const numVotes = data.numvotes; // Usamos 'numvotes' del JSON

    const now = new Date();
    const formattedDate = `${now.toLocaleDateString()} ${now.toLocaleTimeString()}`;

    // Mostrar datos en el HTML
    document.getElementById("average-rating").textContent = `${averageRating}`;
    document.getElementById("num-votes").textContent = `Número de votos: ${numVotes}`;
  } catch (error) {
    console.error("Error:", error);
    document.getElementById("average-rating").textContent = "N/A";
    document.getElementById("num-votes").textContent = "Error al obtener datos.";
  }
}


document.getElementById("refresh-score").addEventListener("click", fetchMovieRating);
////////////////// GRAFICO DE LINEAS //////////////////////

async function buildLineGraph() {
  const response = await fetch(`${API_BASE_URL}/distribution_genres_by_decade`);
  if (!response.ok) throw new Error("Error al obtener los datos de la película");
  const data = await response.json();

  // Agrupar los datos por género y década
  const genres = Array.from(new Set(data.map(d => d.genre)));
  const decades = Array.from(new Set(data.map(d => d.decade))).sort();

  // Establecer márgenes
  const margin = {top: 20, right: 30, bottom: 40, left: 40};

  // Obtener el tamaño del div de contenedor
  const container = d3.select("#genre-distribution-chart");
  const width = container.node().getBoundingClientRect().width;
  const height = 400;  // Puedes ajustar la altura si es necesario

  // Crear el contenedor SVG
  const svg = container.append("svg")
                     .attr("width", width)
                     .attr("height", height)
                   .append("g")
                     .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  // Definir escalas
  const x = d3.scaleBand()
            .domain(decades)
            .range([0, width - margin.left - margin.right])
            .padding(0.1);

  const y = d3.scaleLinear()
            .domain([0, d3.max(data, d => d.count)])
            .nice()
            .range([height - margin.top - margin.bottom, 0]);

  const color = d3.scaleOrdinal(d3.schemeCategory10)
                .domain(genres);

  // Crear las líneas por cada género
  genres.forEach(function(genre) {
    const genreData = data.filter(d => d.genre === genre);
    
    const line = svg.append("path")
                   .datum(genreData)
                   .attr("fill", "none")
                   .attr("stroke", color(genre))
                   .attr("stroke-width", 2)
                   .attr("d", d3.line()
                       .x(d => x(d.decade) + x.bandwidth() / 2)
                       .y(d => y(d.count))
                   )
                   .on("mouseover", function(event, d) {
                     d3.select(this)
                       .transition()
                       .duration(200)
                       .attr("stroke-width", 4);  // Aumenta el grosor de la línea
                     tooltip.transition().duration(200).style("opacity", 1);
                     tooltip.html(genre)  // Muestra el nombre del género
                            .style("left", (event.pageX + 5) + "px")
                            .style("top", (event.pageY - 28) + "px");
                   })
                   .on("mouseout", function() {
                     d3.select(this)
                       .transition()
                       .duration(200)
                       .attr("stroke-width", 2);  // Restaura el grosor original
                     tooltip.transition().duration(200).style("opacity", 0);
                   });
  });

  // Añadir los ejes
  svg.append("g")
  .attr("class", "x axis")
  .attr("transform", "translate(0," + (height - margin.bottom) + ")")
  .call(d3.axisBottom(x)
        .tickValues(decades.filter((d, i) => i % 2 === 0))  // Muestra solo las décadas en índices pares
  );

  svg.append("g")
     .attr("class", "y axis")
     .call(d3.axisLeft(y).tickFormat(d3.format("~s")));  // Formatear números en el eje Y

  // Añadir etiquetas de género con tamaño de fuente ajustado
  svg.append("g")
     .selectAll(".tick")
     .data(genres)
     .enter().append("text")
     .attr("x", 20)
     .attr("y", (d, i) => 30 + i * 20)
     .attr("fill", d => color(d))
     .style("font-size", "12px")  // Reducir el tamaño de las etiquetas
     .text(d => d);

  // Crear el tooltip
  const tooltip = d3.select("body").append("div")
                    .attr("class", "tooltip")
                    .style("position", "absolute")
                    .style("opacity", 0)
                    .style("padding", "5px")
                    .style("background-color", "rgba(0, 0, 0, 0.7)")
                    .style("color", "white")
                    .style("border-radius", "5px");
}

buildLineGraph();



///////////////////MAPA DE CALOR/////////////////////////////////

async function buildHeatmap() {

  const response = await fetch(`${API_BASE_URL}/heatmap_movie_releases`);
  if (!response.ok) throw new Error("Error al obtener los datos de la película");
  const data = await response.json();

  // Configuraciones generales
  const margin = { top: 40, right: 30, bottom: 40, left: 60 };
  const width = document.getElementById("heat-map-chart").offsetWidth - margin.left - margin.right;
  const height = 400 - margin.top - margin.bottom;
  const monthNames = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"];

  // Crear contenedor SVG
  const svg = d3.select("#heat-map-chart")
      .append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  // Escalas
  const x = d3.scaleBand().domain(monthNames).range([0, width]).padding(0.05);
  const y = d3.scaleBand().domain(data.map(d => d.year)).range([0, height]).padding(0.05);
  const color = d3.scaleSequential(d3.interpolateYlOrRd).domain([0, d3.max(data.flatMap(d => d.months))]);

  // Crear celdas del mapa de calor
  const cells = svg.selectAll(".cell")
      .data(data.flatMap(d => d.months.map((count, i) => ({
          year: d.year,
          month: i,
          count: count
      }))))
      .enter().append("rect")
      .attr("class", "cell")
      .attr("x", d => x(monthNames[d.month]))
      .attr("y", d => y(d.year))
      .attr("width", x.bandwidth())
      .attr("height", y.bandwidth())
      .attr("fill", d => color(d.count))
      .on("mouseover", function(event, d) {
          tooltip.style("visibility", "visible")
              .text(`Año: ${d.year}, Mes: ${monthNames[d.month]}, Títulos: ${d.count}`);
          d3.select(this).style("stroke", "#000").style("stroke-width", 2);
      })
      .on("mousemove", function(event) {
          tooltip.style("top", (event.pageY + 10) + "px")
              .style("left", (event.pageX + 10) + "px");
      })
      .on("mouseout", function() {
          tooltip.style("visibility", "hidden");
          d3.select(this).style("stroke", "#ddd").style("stroke-width", 1);
      });

  // Ejes
  svg.append("g")
      .selectAll(".x-axis")
      .data(monthNames)
      .enter()
      .append("text")
      .attr("x", (d, i) => x(d) + x.bandwidth() / 2)
      .attr("y", height)
      .attr("class", "axis-label")
      .text(d => d);

  svg.append("g")
      .selectAll(".y-axis")
      .data(data.map(d => d.year))
      .enter()
      .append("text")
      .attr("x", -10)
      .attr("y", (d, i) => y(d) + y.bandwidth() / 2)
      .attr("class", "axis-label")
      .text(d => d);

  // Tooltip
  const tooltip = d3.select("body").append("div").attr("class", "tooltip");
}

buildHeatmap();

///////////////ESCRITORES DIRECTORES//////////////////////////////////7
async function createHeatmap() {

  const response = await fetch(`${API_BASE_URL}/collaboration_heatmap`);
  if (!response.ok) throw new Error("Error al obtener los datos de la película");
  const data = await response.json();

  const margin = { top: 50, right: 50, bottom: 100, left: 100 },
        width = 900 - margin.left - margin.right,
        height = 600 - margin.top - margin.bottom;

  const svg = d3.select("#heatmap")
      .append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  // Extraer los actores, directores, escritores y las calificaciones
  const actorsDirectors = Array.from(new Set(data.map(d => d.actor_director)));
  const collaborators = Array.from(new Set(data.map(d => d.collaborator)));

  const colorScale = d3.scaleSequential(d3.interpolateBlues)
      .domain([0, d3.max(data, d => d.averageRating)]);

  // Escalas para las posiciones X y Y
  const xScale = d3.scaleBand()
      .range([0, width])
      .domain(actorsDirectors)
      .padding(0.05);

  const yScale = d3.scaleBand()
      .range([height, 0])
      .domain(collaborators)
      .padding(0.05);

  // Añadir las columnas (actores/directores)
  svg.append("g")
      .selectAll(".x-axis")
      .data(actorsDirectors)
      .enter().append("text")
      .attr("class", "x-axis")
      .attr("x", (d, i) => xScale(d) + xScale.bandwidth() / 2)
      .attr("y", height + 40)
      .attr("text-anchor", "middle")
      .text(d => d)
      .style("font-size", "12px")
      .style("transform", "rotate(45deg)")
      .style("transform-origin", "center center");

  // Añadir las filas (colaboradores)
  svg.append("g")
      .selectAll(".y-axis")
      .data(collaborators)
      .enter().append("text")
      .attr("class", "y-axis")
      .attr("x", -50)
      .attr("y", d => yScale(d) + yScale.bandwidth() / 2)
      .attr("dy", ".35em")
      .attr("text-anchor", "middle")
      .text(d => d)
      .style("font-size", "12px")
      .style("writing-mode", "vertical-rl");

  // Crear las celdas de la matriz de calor
  svg.append("g")
      .selectAll(".cell")
      .data(data)
      .enter().append("rect")
      .attr("class", "cell")
      .attr("x", d => xScale(d.actor_director))
      .attr("y", d => yScale(d.collaborator))
      .attr("width", xScale.bandwidth())
      .attr("height", yScale.bandwidth())
      .attr("fill", d => colorScale(d.averageRating))
      .on("mouseover", function(event, d) {
          d3.select(this).style("stroke", "#000").style("stroke-width", 2);
          // Mostrar la calificación promedio al pasar el ratón
          tooltip.transition().duration(200).style("opacity", .9);
          tooltip.html(d.actor_director + " & " + d.collaborator + ": " + d.averageRating.toFixed(2))
              .style("left", (event.pageX + 5) + "px")
              .style("top", (event.pageY - 28) + "px");
      })
      .on("mouseout", function(d) {
          d3.select(this).style("stroke", "none");
          tooltip.transition().duration(500).style("opacity", 0);
      });

  // Crear el tooltip
  const tooltip = d3.select("body").append("div")
      .attr("class", "tooltip")
      .style("position", "absolute")
      .style("text-align", "center")
      .style("padding", "5px")
      .style("background", "rgba(0,0,0,0.7)")
      .style("color", "white")
      .style("border-radius", "5px")
      .style("opacity", 0);

}
createHeatmap();