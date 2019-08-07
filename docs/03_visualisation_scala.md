---
permalink: /docs/visualisation/scala/
layout: splash
title: "Tutorial: Visualisation"
date: 2018-06-18 22:31:13 +0200
---

# Tutorial: Visualise your partitioning

In many cases, we would like to perform some visual inspection of the partitioning, but displaying large data sets is not a trivial task. The approach here consists in repartitioning the data set, and sampling the data for the display. The colour code corresponds to partition ID.

## Before starting

We provide a script based on the [Scala client of Plotly](https://github.com/facultyai/scala-plotly-client). Unfortunately, the releases on Maven are not up-to-date (e.g. they do not contain the code to make Scatter3D plots), hence we compiled the latest sources and released the JAR under `$spark3d/lib`. You will also find a bash script to help running it:

```bash
# in $spark3d
./run_viz_scala.sh
```

Just adapt the input parameters according to your data and cluster configuration. You will also need to open an account on plotly, and provide your username and api key in the bash script. The plots will then be available in your plotly home.

## Some examples

### Onion repartitioning

We repartitioned a dataset containing data in spherical coordinates using the `onion` partitioning scheme. Only a fraction of the total dataset is used for plot:

<div>
    <a href="https://plot.ly/~JulienPeloton/223/?share_key=AWsciidRkVB4Bb4Ur2WJQg" target="_blank" title="partitioning-out_srcs_s1_1*.fits-onion-spark3dweb" style="display: block; text-align: center;"><img src="https://plot.ly/~JulienPeloton/223.png?share_key=AWsciidRkVB4Bb4Ur2WJQg" alt="partitioning-out_srcs_s1_1*.fits-onion-spark3dweb" style="max-width: 100%;width: 600px;"  width="600" onerror="this.onerror=null;this.src='https://plot.ly/404.png';" /></a>
    <script data-plotly="JulienPeloton:223" sharekey-plotly="AWsciidRkVB4Bb4Ur2WJQg" src="https://plot.ly/embed.js" async></script>
</div>

### Octree repartitioning

We repartitioned a dataset containing data in cartesian coordinates using the `octree` partitioning scheme. Only a fraction of the total dataset is used for plot:

<div>
    <a href="https://plot.ly/~JulienPeloton/225/?share_key=01FRN2snL0QWpSHefu2PqH" target="_blank" title="partitioning-dc2-octree-spark3dweb" style="display: block; text-align: center;"><img src="https://plot.ly/~JulienPeloton/225.png?share_key=01FRN2snL0QWpSHefu2PqH" alt="partitioning-dc2-octree-spark3dweb" style="max-width: 100%;width: 600px;"  width="600" onerror="this.onerror=null;this.src='https://plot.ly/404.png';" /></a>
    <script data-plotly="JulienPeloton:225" sharekey-plotly="01FRN2snL0QWpSHefu2PqH" src="https://plot.ly/embed.js" async></script>
</div>

Here is another data set with clusters:

<div>
    <a href="https://plot.ly/~JulienPeloton/233/?share_key=Vj7gfS7eTl8A96ot9mxU13" target="_blank" title="partitioning-dc2_rand-octree-spark3web" style="display: block; text-align: center;"><img src="https://plot.ly/~JulienPeloton/233.png?share_key=Vj7gfS7eTl8A96ot9mxU13" alt="partitioning-dc2_rand-octree-spark3web" style="max-width: 100%;width: 600px;"  width="600" onerror="this.onerror=null;this.src='https://plot.ly/404.png';" /></a>
    <script data-plotly="JulienPeloton:233" sharekey-plotly="Vj7gfS7eTl8A96ot9mxU13" src="https://plot.ly/embed.js" async></script>
</div>

