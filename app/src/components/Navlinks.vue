<template>
  <div class="navlinks">
    <ul>
      <li><router-link :to="{name: 'home'}">Home</router-link></li>
      <li><router-link :to="{name: 'getstarted'}">Get Started</router-link></li>
      <li><router-link :to="{name: 'patienttree'}">Patient Tree</router-link></li>
    </ul>
  </div>
</template>
  
<script>
export default {
  name: 'template-nav'
}
</script>

<style lang="scss" scoped>
.navlinks {
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
        color: currentColor;
      }
    }
  }
}
</style>
