<template>
  <div class="__page">
    <div class="__pseudo__nav">
      <ul>
        <li><router-link to="/">Home</router-link></li>
        <li><router-link to="/get-started">Get Started</router-link></li>
        <li><router-link to="/patient-tree">Patient Tree</router-link></li>
      </ul>
    </div>
    <slot></slot>
    <div class="__footer">
      <img :src="MolgenisLogo" alt="molgenis open source data platform" class="molgenis-logo"/>
      <slot name="footer"></slot>
    </div>
  </div>
</template>

<script>
export default {
  name: 'uiPage',
  data () {
    return {
      MolgenisLogo: require('../assets/molgenis-logo-blue-small.png')
    }
  }
}
</script>

<style lang="scss">
.mg-page,
.mg-app,
.container-fluid,
.row,
.col-sm-8,
.col-sm-3 {
  padding: 0;
}

.mg-page .mg-page-content {
  margin-top: 0;
}

footer.footer {
  display: none;
}

.__page {
  min-height: 100vh;
  background-color: #f6f6f6;
  
  .__footer {
    padding: 2em 1em;
    background-color: #282d32;
    
    .molgenis-logo {
      width: 124px;
    }
  }
}

.__pseudo__nav {
  ul {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    justify-content: flex-start;
    align-items: center;
    
    li {
      box-sizing: content-box;
      padding: 0.5em 1em;
      
      a {
        text-decoration: underline;
      }
    }
  }
}

</style>
