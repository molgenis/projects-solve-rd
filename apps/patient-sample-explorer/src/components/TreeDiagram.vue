<template>
  <div class="tree-container">
    <h3 :id="id-title" class="tree-title" v-if="title">{{ title }}</h3>
    <ul :id="id" role="tree" class="tree-hierarchy tree-root" :aria-labelledby="id-title"></ul>
  </div>
</template>

<script>
import * as d3 from 'd3'

// <svg xmlns="http://www.w3.org/2000/svg" class="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
//   <path stroke-linecap="round" stroke-linejoin="round" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
// </svg>

export default {
  props: {
    id: {
      type: String,
      required: true
    },
    title: {
      type: String,
      requre: true
    },
    data: {
      type: Object,
      required: true
    }
  },
  data () {
    return {
      iconFileDefaultPath: 'M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z',
      iconFileClosedPath: 'M9 13h6m-3-3v6m-9 1V7a2 2 0 012-2h6l2 2h6a2 2 0 012 2v8a2 2 0 01-2 2H5a2 2 0 01-2-2z',
      iconFileOpenPath: 'M9 13h6M3 17V7a2 2 0 012-2h6l2 2h6a2 2 0 012 2v8a2 2 0 01-2 2H5a2 2 0 01-2-2z',
      iconDocumentPath: 'M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z'
    }
  },
  methods: {
    mouseover (event) {
      const toggle = event.target
      const parentElem = toggle.parentNode
      parentElem.classList.add('focused')
    },
    mouseout (event) {
      const toggle = event.target
      const parentElem = toggle.parentNode
      parentElem.classList.remove('focused')
    },
    insertFolderIcon (element, path, size) {
      element.append('svg')
        .attr('xmlns', 'http://www.w3.org/2000/svg')
        .attr('class', 'tree-item-icon')
        .attr('aria-hidden', 'true')
        .attr('width', size)
        .attr('height', size)
        .attr('fill', 'none')
        .attr('viewBox', '0 0 24 24')
        .attr('stroke', 'currentColor')
        .attr('stroke-width', '2')
        .append('path')
        .attr('stroke-linecap', 'round')
        .attr('stroke-linejoin', 'round')
        .attr('d', path)
    },
    click (event) {
      const toggle = event.target
      const toggleIcon = event.target.previousElementSibling
      const toggleIconPath = toggleIcon.firstChild
      const subtree = event.target.nextElementSibling

      if (subtree.hasAttribute('aria-hidden')) {
        toggle.setAttribute('aria-expanded', true)
        subtree.removeAttribute('aria-hidden')
        toggleIconPath.setAttribute('d', this.iconFileOpenPath)
      } else {
        toggle.setAttribute('aria-expanded', false)
        subtree.setAttribute('aria-hidden', true)
        toggleIconPath.setAttribute('d', this.iconFileClosedPath)
      }
    },
    buildTreeItem (tree, node) {
      const listItem = tree.append('li')
        .attr('class', 'tree-item')
        .attr('data-tree-level', node.level)
        .attr('data-tree-group', node.group)
      
      if (node.level === 1) {
        listItem.attr('role', 'treeitem')
      }
      
      if (node.children) {
        this.insertFolderIcon(listItem, this.iconFileClosedPath, 24)
        listItem.append('button')
          .attr('id', node.id)
          .attr('class', 'subtree-toggle')
          .attr('aria-expanded', false)
          .attr('aria-controls', `subtree-${node.id}`)
          .text(node.name)
          .on('click', this.click)
          .on('mouseover', this.mouseover)
          .on('mouseout', this.mouseout)
          .on('focus', this.mouseover)
          .on('blur', this.mouseout)
          
        const subtree = listItem.append('ul')
          .attr('id', `subtree-${node.id}`)
          .attr('role', 'group')
          .attr('class', 'subtree-hierarchy')
        
        if (node.level > 0) {
          subtree.attr('aria-hidden', 'true')
        }

        node.children.forEach(child => this.buildTreeItem(subtree, child))
      } else {
        if (node.level > 1) {
          this.insertFolderIcon(listItem, this.iconDocumentPath, 24)
        } else {
          this.insertFolderIcon(listItem, this.iconFileDefaultPath, 24)
        }
        listItem.append('span')
          .attr('class', 'tree-item-label')
          .text(node.name)
      }
    },
    renderTree () {
      const treeElemId = this.$el.childNodes[1].id
      const tree = d3.select(`#${treeElemId}`)
      this.tree = tree
      
      const children = this.data.children
      children.forEach(child => this.buildTreeItem(this.tree, child))
    }
  },
  mounted () {
    this.renderTree()
  }
}
</script>

<style lang="scss">
.tree-hierarchy {
  padding: 0;
  margin: 0;
  list-style: none;
  position: relative;
  
  .subtree-hierarchy {
    list-style: none;

    &[aria-hidden=true] {
      display: none;
    }
  }
  
  .tree-item {
    margin-bottom: 12px;
    background-color: transparent;
    
    .tree-item-icon {
      margin-top: -2px;
      color: #707070;
    }
    
    .tree-item-label {
      letter-spacing: 0.02em;
    }

    .subtree-toggle {
      border: none;
      padding: 0;
      margin: 0;
      background: none;
      background-color: none;
      color: currentColor;
      border-radius: 6px;
    }
    
    .tree-item-label,
    .subtree-toggle {
      padding: 2px 8px;
    }
    
    &.focused {
      > .subtree-toggle {
        background-color: #3f454b;
        color: white;
      }
    }
  }
}
</style>
