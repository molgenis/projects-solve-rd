<template>
  <div class="d3-viz-container">
    <svg
      :id="chartId"
      :width="chartWidth"
      :height="chartHeight"
      :viewBox="`0 0 ${chartWidth} ${chartHeight}`"
      preserveAspectRatio="xMinYMin"
      class="d3-viz d3-dendogram"
    ></svg>
  </div>
</template>

<script>
import * as d3 from 'd3'

export default {
  props: {
    chartId: {
      type: String,
      required: true
    },
    chartData: {
      type: Object,
      required: true
    },
    chartWidth: {
      type: Number,
      default: 900
    },
    chartHeight: {
      type: Number,
      default: 500
    },
    chartMargins: {
      type: Object,
      default: () => ({
        top: 20,
        bottom: 30,
        right: 90,
        left: 90
      })
    }
  },
  methods: {
    collapseNode (node) {
      if (node.children) {
        node._children = node.children
        node._children.forEach(this.collapseNode)
        node.children = null
      }
    },
    drawPath (parent, child) {
      const path = `M ${parent.y} ${parent.x}
                    C ${(parent.y + child.y) / 2} ${parent.x},
                    ${(parent.y + child.y) / 2} ${child.x},
                    ${child.y} ${child.x}`
      return path
    },
    onclick (event, d) {
      if (d.children) {
        d._children = d.children
        d.children = null
      } else {
        d.children = d._children
        d._children = null
      }
      this.updateChart(d)
    },
    updateChart (source) {
      const treeData = this.treeMap(this.root)
      const nodes = treeData.descendants()
      const links = treeData.descendants().slice(1)
      nodes.forEach(d => { d.y = d.depth * 180 }) // normalize nodes

      const node = this.svg.selectAll('g.node')
        .data(nodes, (d, index) => {
          d.id = index
        })
      
      // enter new nodes at the parent's previous position
      const nodeEnter = node.enter()
        .append('g')
        .attr('class', 'node-group')
        .attr('transform', d => `translate(${source.y0}, ${source.x0})`)
        .on('click', this.onclick)
        
      nodeEnter.append('circle')
        .attr('class', 'node')
        .attr('r', 1e-6)
        .style('fill', d => d._children ? 'lightsteelblue' : '#ffffff')
        
      nodeEnter.append('text')
        .attr('class', 'node-label')
        .attr('dy', '0.35em')
        .attr('x', d => {
          return d.children || d._children ? -16 : 16
        })
        .attr('text-anchor', d => {
          return d.children || d._children ? 'end' : 'start'
        })
        .text(d => d.data.name)

      // update node
      const nodeUpdate = nodeEnter.merge(node)
      nodeUpdate.transition()
        .duration(this.duration)
        .attr('transform', d => `translate(${d.y}, ${d.x})`)
        
      nodeUpdate.select('circle.node')
        .attr('r', 10)
        .style('fill', d => d._children ? 'lightsteelblue' : '#ffffff')
        .attr('cursor', 'pointer')
        
      const nodeExit = node.exit().transition()
        .duration(this.duration)
        .attr('transform', (d) => `translate(${source.y}, ${source.x})`)
        .remove()
        
      // on exit modifications
      nodeExit.select('circle').attr('r', 1e-6)
      nodeExit.select('text').style('fill-opacity', 1e-6)
      
      const link = this.svg.selectAll('path.link')
        .data(links, link => link.id)
      
      const linkEnter = link.enter()
        .insert('path', 'g')
        .attr('class', 'link')
        .attr('fill', 'none')
        .attr('stroke', '#cccccc')
        .attr('stroke-width', 2)
        .attr('d', () => {
          const coords = { x: source.x0, y: source.y0 }
          return this.drawPath(coords, coords)
        })
      
      const linkUpdate = linkEnter.merge(link)
      linkUpdate.transition()
        .duration(this.duration)
        .attr('d', d => this.drawPath(d, d.parent))
        
      link.exit().transition()
        .duration(this.duration)
        .attr('d', () => {
          const coords = { x: source.x, y: source.y }
          return this.drawPath(coords, coords)
        })
        .remove()
        
      nodes.forEach(node => {
        node.x0 = node.x
        node.y0 = node.y
      })
    },
    renderChart () {
      const svg = d3.select(`#${this.$el.childNodes[0].id}`)
        .append('g')
        .attr('transform', `translate(${this.chartMargins.left}, ${this.chartMargins.top})`)
        .style('user-select', 'none')
      this.svg = svg
      
      const treeMap = d3.tree()
        .size([
          this.chartHeight - this.chartMargins.top - this.chartMargins.bottom,
          this.chartWidth - this.chartMargins.left - this.chartMargins.right
        ])
      this.treeMap = treeMap
        
      const root = d3.hierarchy(this.chartData, child => child.children)
      root.x0 = this.chartHeight / 2
      root.y0 = 0
      root.children.forEach(this.collapseNode)
      this.root = root

      this.updateChart(this.root)
    }
  }//,
  // mounted () {
  //   this.renderChart()
  // }
}
</script>

<style lang="scss">
svg {
  border: 1px solid blue;
}
</style>
