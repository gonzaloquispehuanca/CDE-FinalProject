// Constante para la API
const EMR_HOST = "ec2-44-200-199-44.compute-1.amazonaws.com";
const KAFKA_PRODUCER_HOST = "ec2-18-207-193-119.compute-1.amazonaws.com";
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
  const margin = {top: 5, right: 5, bottom: 40, left: 40};

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

async function renderVotesChart(containerId, tooltipId) {
  const response = await fetch(`${API_BASE_URL}/votes_by_content_type`);
  if (!response.ok) throw new Error("Error al obtener los datos de la película");
  const data = await response.json();

  const container = d3.select(`#${containerId}`);
  const tooltip = d3.select(`#${tooltipId}`);
  container.select("svg").remove();

  const width = container.node().getBoundingClientRect().width;
  const height = container.node().getBoundingClientRect().height;

  const svg = container.append("svg").attr("width", width).attr("height", height);

  const margin = { top: 20, right: 30, bottom: 50, left: 100 };
  const chartWidth = width - margin.left - margin.right;
  const chartHeight = height - margin.top - margin.bottom;

  const g = svg.append("g").attr("transform", `translate(${margin.left}, ${margin.top})`);

  const x = d3.scaleLinear().domain([0, d3.max(data, (d) => d.totalVotes)]).range([0, chartWidth]);
  const y = d3.scaleBand().domain(data.map((d) => d.titleType)).range([0, chartHeight]).padding(0.2);

  const xAxis = d3.axisBottom(x).ticks(5).tickFormat(d3.format(".2s"));
  const yAxis = d3.axisLeft(y);

  g.append("g").attr("transform", `translate(0, ${chartHeight})`).call(xAxis);
  g.append("g").call(yAxis);

  g.selectAll(".bar")
    .data(data)
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr("y", (d) => y(d.titleType))
    .attr("height", y.bandwidth())
    .attr("x", 0)
    .attr("width", 0)
    .attr("fill", "#69b3a2")
    .on("mouseover", function (event, d) {
      d3.select(this).attr("fill", "#217a6c");
      tooltip
        .style("visibility", "visible")
        .html(`<strong>${d.titleType}</strong>: ${d3.format(",")(d.totalVotes)} votes`)
        .style("left", event.pageX + 10 + "px")
        .style("top", event.pageY - 28 + "px");
    })
    .on("mousemove", function (event) {
      tooltip.style("left", event.pageX + 10 + "px").style("top", event.pageY - 28 + "px");
    })
    .on("mouseout", function () {
      d3.select(this).attr("fill", "#69b3a2");
      tooltip.style("visibility", "hidden");
    })
    .transition()
    .duration(1000)
    .attr("width", (d) => x(d.totalVotes));

  g.selectAll(".label")
    .data(data)
    .enter()
    .append("text")
    .attr("class", "label")
    .attr("x", (d) => x(d.totalVotes) + 5)
    .attr("y", (d) => y(d.titleType) + y.bandwidth() / 2)
    .attr("dy", ".35em")
    .text((d) => d3.format(".2s")(d.totalVotes));
}


renderVotesChart("votes-chart", "votes-tooltip");

window.addEventListener("resize", () => {
  renderVotesChart("votes-chart", "votes-tooltip");
});

///////////////ESCRITORES DIRECTORES//////////////////////////////////7
async function renderDirectorsChart(containerId, tooltipId) {

  const response = await fetch(`${API_BASE_URL}/top_directors_weighted`);
  if (!response.ok) throw new Error("Error al obtener los datos de la película");
  const data = await response.json();


  const container = d3.select(`#${containerId}`);
  const tooltip = d3.select(`#${tooltipId}`);
  container.select("svg").remove();

  // Dimensiones dinámicas
  const containerWidth = container.node().getBoundingClientRect().width;
  const containerHeight = container.node().getBoundingClientRect().height;

  const width = containerWidth;
  const height = containerHeight;
  const margin = { top: 40, right: 30, bottom: 100, left: 120 };
  const chartWidth = width - margin.left - margin.right;
  const chartHeight = height - margin.top - margin.bottom;

  // Crear SVG
  const svg = container
    .append("svg")
    .attr("width", width)
    .attr("height", height);

  const g = svg
    .append("g")
    .attr("transform", `translate(${margin.left}, ${margin.top})`);

  // Ordenar datos por weightedScore
  data.sort((a, b) => b.weightedScore - a.weightedScore);

  // Escalas
  const x = d3.scaleLinear()
    .domain([0, d3.max(data, d => d.weightedScore)])
    .range([0, chartWidth]);

  const y = d3.scaleBand()
    .domain(data.map(d => d.director))
    .range([0, chartHeight])
    .padding(0.2);

  // Ejes
  const xAxis = d3.axisBottom(x).ticks(5).tickFormat(d3.format(".2f"));
  const yAxis = d3.axisLeft(y);

  g.append("g")
    .attr("transform", `translate(0, ${chartHeight})`)
    .call(xAxis);

  g.append("g").call(yAxis);

  // Barras
  const bars = g.selectAll(".bar")
    .data(data)
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr("y", d => y(d.director))
    .attr("height", y.bandwidth())
    .attr("x", 0)
    .attr("width", 0)
    .attr("fill", "#4A90E2")
    .on("mouseover", function (event, d) {
      d3.select(this).attr("fill", "#1C5A99");
      tooltip
        .style("visibility", "visible")
        .html(`
          <strong>${d.director}</strong><br>
          Calificación promedio: ${d3.format(".2f")(d.averageRating)}<br>
          Películas dirigidas: ${d.movieCount}<br>
          Puntuación: ${d3.format(".2f")(d.weightedScore)}
        `)
        .style("left", event.pageX + 10 + "px")
        .style("top", event.pageY - 28 + "px");
    })
    .on("mousemove", function (event) {
      const tooltipWidth = tooltip.node().offsetWidth;
      const tooltipHeight = tooltip.node().offsetHeight;
      const pageWidth = document.body.clientWidth;

      const xOffset = event.pageX + tooltipWidth > pageWidth ? -tooltipWidth - 10 : 10;
      const yOffset = event.pageY + tooltipHeight > window.innerHeight ? -tooltipHeight - 10 : -28;

      tooltip
        .style("left", event.pageX + xOffset + "px")
        .style("top", event.pageY + yOffset + "px");
    })
    .on("mouseout", function () {
      d3.select(this).attr("fill", "#4A90E2");
      tooltip.style("visibility", "hidden");
    });

  bars.transition()
    .duration(800)
    .attr("width", d => x(d.weightedScore))
    .delay((_, i) => i * 50);

  // Etiquetas
  g.selectAll(".label")
    .data(data)
    .enter()
    .append("text")
    .attr("class", "label")
    .attr("x", 0)
    .attr("y", d => y(d.director) + y.bandwidth() / 2)
    .attr("dx", "5px")
    .attr("dy", "0.35em")
    .attr("fill", "white")
    .attr("font-size", "12px")
    .text(d => d3.format(".2f")(d.weightedScore))
    .transition()
    .duration(800)
    .attr("x", d => x(d.weightedScore) - 10)
    .delay((_, i) => i * 50);
}

renderDirectorsChart("directors-chart", "directors-tooltip");
