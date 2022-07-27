import { createRouter, createWebHistory } from 'vue-router'

import Home from '@/pages/Home.vue'
import GetStarted from '@/pages/GetStarted.vue'
import PatientTree from '@/pages/PatientTree.vue'

const initialState = window.__INITIAL_STATE__ || {}

const routes = [
  {
    name: 'home',
    path: '/',
    component: Home
  },
  {
    name: 'getstarted',
    path: '/get-started',
    component: GetStarted
  },
  {
    name: 'patienttree',
    path: '/patient-tree',
    component: PatientTree
  }
]

const router = createRouter({
  history: createWebHistory(initialState.baseUrl),
  routes,
  scrollBehavior (to, from, savedPosition) {
    return {
      top: 0
    }
  }
})

export default router
