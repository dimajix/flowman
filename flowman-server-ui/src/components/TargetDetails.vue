<template>
  <v-container fluid>
    <v-card-title>
      <v-icon>gavel</v-icon>
      Target '{{details.project}}/{{details.name}}' {{ details.phase }} id {{target}} status {{details.status}}
    </v-card-title>
    <v-card-text>
      <screen id="screen" ref="screen" :markers="[]" height="600">
        <edge v-for="edge in graph.edges" :data="edge" :nodes="graph.nodes" :key="edge.id">
        </edge>

        <node v-for="node in graph.nodes" :data="node" :key="node.id" :class="node.category">
          <v-card-text
            @mouseenter="(e) => onMouseEnter(e, node)"
            @mouseover="onMouseOver"
            @mouseleave="onMouseLeave"
          >
            <v-container>
              <v-row justify="center">{{node.category}}/{{node.kind}}</v-row>
              <v-row justify="center"><h4>{{node.name}}</h4></v-row>
            </v-container>
          </v-card-text>
        </node>
      </screen>
      <v-tooltip
        v-model="showTooltip"
        absolute
        bottom
        :position-x="tooltipX"
        :position-y="tooltipY"
      >
        <v-container>
          <v-row justify="center">{{tooltipNode.category}}/{{tooltipNode.kind}}</v-row>
          <v-row justify="center"><h2>{{tooltipNode.name}}</h2></v-row>
        </v-container>
        <v-container>
          <v-row><h3>Provides</h3></v-row>
          <v-row>
            <resource-table :resources="tooltipNode.provides"/>
          </v-row>
          <v-row><h3>Requires</h3></v-row>
          <v-row>
            <resource-table :resources="tooltipNode.requires"/>
          </v-row>
        </v-container>
      </v-tooltip>
    </v-card-text>
  </v-container>
</template>

<script>
import graph from 'vnodes/src/graph'
import Screen from 'vnodes/src/components/Screen'
import Node from 'vnodes/src/components/Node'
import Edge from 'vnodes/src/components/Edge'
import ResourceTable from "@/components/ResourceTable";

export default {
  name: 'TargetDetails',
  components: {Screen,Node,Edge,ResourceTable},

  props: {
    target: String
  },

  data () {
    return {
      details: {},
      targetGraph: {},
      graph: new graph(),
      showTooltip: false,
      tooltipNode: {
        requires: [],
        provides: []
      },
      tooltipX: 0,
      tooltipY: 0
    }
  },

  mounted() {
    this.refresh()
  },

  methods: {
    refresh() {
      this.$api.getTargetDetails(this.target).then(response => {
        this.details = response
      })
      this.$api.getTargetGraph(this.target).then(response => {
        this.targetGraph = response

        this.createNodes(response)
        this.centerNodes()
      })
    },

    createNodes(response) {
        this.graph.reset()

        let g = this.graph
        response.nodes.forEach(node => {
          g.createNode({
            id: node.id.toString(),
            category: node.category,
            kind: node.kind,
            name: node.name,
            provides: node.provides,
            requires: node.requires
          })
        });
        response.edges.forEach(edge => {
          g.createEdge({
            from: edge.input.toString(),
            to: edge.output.toString()
          })
        });

        this.graph.edges.forEach(edge => {
          this.$set(edge.fromAnchor, 'snap', 'rect')
          this.$set(edge.toAnchor, 'snap', 'rect')
        })

      this.graph.graphNodes({
        nodes: this.graph.nodes,
        edges: this.graph.edges,
        type: 'basic',
        dir: 'right',
        spacing: 220
      })
    },

    centerNodes () {
      const panzoom = this.$refs.screen.panzoom
      this.zoomNodes()
      if (panzoom.getZoom() > 1) {
        this.zoomNodes(1) // fix, only allow zoom out
      }
    },
    zoomNodes (scale=null) {
      let left = Infinity
      let top = Infinity
      let right = -Infinity
      let bottom = -Infinity
      const nodes = this.filterNodes || this.graph.nodes
      nodes.forEach(node => {
        if (node.x < left) left = node.x
        if (node.x + node.width > right) right = node.x + node.width
        if (node.y < top) top = node.y
        if (node.y + node.height > bottom) bottom = node.y + node.height
      })
      left -= 50
      top -= 50
      right += 50
      bottom += 50
      this.$refs.screen.zoomRect({ left, top, right, bottom }, { scale })
    },

    onMouseEnter(e, node) {
      this.tooltipX = e.clientX
      this.tooltipY = e.clientY
      this.showTooltip = true
      this.tooltipNode = node
    },
    onMouseOver(e) {
      this.tooltipX = e.clientX
      this.tooltipY = e.clientY
    },
    onMouseLeave() {
      this.showTooltip = false
    }
  }
}
</script>


<style>
#screen .node.mapping .content {
  background-color: mediumseagreen;
}
#screen .node.target .content {
  background-color: thistle;
}
#screen .node.relation .content {
  background-color: lightsteelblue;
}
#screen .node .content:hover {
  background-color: lightcoral;
}
</style>
